/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"context"
	"fmt"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// a new global cluster cache to handle cluster scoped resources.
const globalClusterCache = "_cluster"

// [_cluster -> cache ("*"), clusterNames -> caches ]

// MultiClusterCacheBuilder - Builder function to create a new multi-cluster cache.
// This will scope the cache to a list of clusters. Listing for all clusters
// will list for all the clusters that this knows about. By default this will create
// a global cache for all cluster resource.
func MultiClusterCacheBuilder(clusterNames []string) NewCacheFunc {
	return func(config *rest.Config, opts Options) (Cache, error) {
		opts, err := defaultOpts(config, opts)
		if err != nil {
			return nil, err
		}

		caches := map[string]Cache{}

		// create aglobal cache for * scope
		globalConfig := *config
		globalConfig.Host = globalConfig.Host + "/clusters/*"
		gCache, err := New(&globalConfig, opts)
		if err != nil {
			return nil, fmt.Errorf("error creating global cache %v", err)
		}

		for _, cs := range clusterNames {
			scopedConfig := *config
			scopedConfig.Host = config.Host + "/clusters/" + cs
			opts.ClusterName = cs
			c, err := New(&scopedConfig, opts)
			if err != nil {
				return nil, err
			}
			caches[cs] = c
		}
		return &multiClusterCache{clusterToCache: caches, Scheme: opts.Scheme, RESTMapper: opts.Mapper, gClusterCache: gCache, cfg: *config, opts: opts}, nil
	}
}

// multiClusterCache knows how to handle multiple namespaced caches
// Use this feature when scoping permissions for your
// operator to a list of namespaces instead of watching every namespace
// in the cluster.
type multiClusterCache struct {
	clusterToCache map[string]Cache
	Scheme         *runtime.Scheme
	RESTMapper     apimeta.RESTMapper
	gClusterCache  Cache // Point to "*"
	cfg            rest.Config
	opts           Options
}

var _ Cache = &multiClusterCache{}

// Methods for multiClusterCache to conform to the Informers interface.
func (c *multiClusterCache) GetInformer(ctx context.Context, obj client.Object) (Informer, error) {
	informers := map[string]Informer{}

	//get CLusterName
	clusterName := getClusterName(ctx, obj)

	if (clusterName) == "*" {
		globalInformer, err := c.gClusterCache.GetInformer(ctx, obj)
		if err != nil {
			return nil, err
		}
		informers[globalClusterCache] = globalInformer
	}
	obj.SetClusterName(clusterName)

	for cs, cache := range c.clusterToCache {
		informer, err := cache.GetInformer(ctx, obj)
		if err != nil {
			return nil, err
		}
		informers[cs] = informer
	}

	return &multiClusterInformer{clusterNameToInformer: informers}, nil

}

func getClusterName(ctx context.Context, obj client.Object) string {
	clusterName := obj.GetClusterName()
	if clusterName == "" {
		clusterName, _ = ctx.Value("clusterName").(string)
	}

	if clusterName == "" {
		return "*"
	}

	return clusterName
}

func (c *multiClusterCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind) (Informer, error) {
	return nil, fmt.Errorf("not supported in multiClustercache")
}

func (c *multiClusterCache) Start(ctx context.Context) error {
	// start global cache
	go func() {
		err := c.gClusterCache.Start(ctx)
		if err != nil {
			log.Error(err, "cluster scoped cache failed to start")
		}
	}()

	// start namespaced caches
	for cs, cache := range c.clusterToCache {
		go func(cs string, cache Cache) {
			err := cache.Start(ctx)
			if err != nil {
				log.Error(err, "multiClusterCache cache failed to start cluster informer", "cluster", cs)
			}
		}(cs, cache)
	}

	<-ctx.Done()
	return nil
}

func (c *multiClusterCache) WaitForCacheSync(ctx context.Context) bool {
	synced := true
	for _, cache := range c.clusterToCache {
		if s := cache.WaitForCacheSync(ctx); !s {
			synced = s
		}
	}

	// check if global cluster cache has synced
	if !c.gClusterCache.WaitForCacheSync(ctx) {
		synced = false
	}
	return synced
}

func (c *multiClusterCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {

	clusterName := getClusterName(ctx, obj)

	if clusterName == "*" {
		return c.gClusterCache.IndexField(ctx, obj, field, extractValue)
	}
	obj.SetClusterName(clusterName)

	for _, cache := range c.clusterToCache {
		if err := cache.IndexField(ctx, obj, field, extractValue); err != nil {
			return err
		}
	}
	return nil
}

func (c *multiClusterCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	clusterName := getClusterName(ctx, obj)

	if clusterName == "*" {
		// Look into the global cache to fetch the object
		return c.gClusterCache.Get(ctx, key, obj)
	}
	obj.SetClusterName(clusterName)

	cache, ok := c.clusterToCache[clusterName]
	if !ok {
		scopedConfig := c.cfg
		scopedConfig.Host = c.cfg.Host + "/clusters/" + clusterName
		c.opts.ClusterName = clusterName
		newCache, err := New(&scopedConfig, c.opts)
		if err != nil {
			return err
		}
		c.clusterToCache[clusterName] = newCache
		cache = newCache
		go func(cs string, cache Cache) {
			// TODO this is totally wrong, cache.Start blocks
			// How do we dynamically start caches as requests to new clusters come in?
			err := cache.Start(ctx)
			if err != nil {
				log.Error(err, "multiClusterCache cache failed to start cluster informer", "cluster", cs)
			}
		}(clusterName, newCache)
	}

	return cache.Get(ctx, key, obj)
}

// List
// ClusterName is not passed => error
// ClusterName is passed => getCache
// ListAll clusters => clusterName is "*"

func (c *multiClusterCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := client.ListOptions{}

	clusterName := listOpts.ClusterName
	if clusterName == "" {
		clusterName, _ = ctx.Value("clusterName").(string)
	}
	if clusterName == "" {
		// initial stab - error out
		fmt.Errorf("cluster Name is empty in listOpts")
	}

	listOpts.ApplyOptions(opts)

	if clusterName == "*" {
		// Look at gloabal cluster cache
		return c.gClusterCache.List(ctx, list, opts...)
	}

	// look at individual caches
	cache, ok := c.clusterToCache[clusterName]
	if !ok { // cache is not found to the particular cluster
		return fmt.Errorf("unable to get cache because clusterName %v is not known", clusterName)
	}
	return cache.List(ctx, list, opts...)
}

// informer maps
type multiClusterInformer struct {
	clusterNameToInformer map[string]Informer
}

var _Informer = &multiClusterInformer{}

// AddEventHandler adds the handler to each cluster scoped informer.
func (i *multiClusterInformer) AddEventHandler(handler toolscache.ResourceEventHandler) {
	for _, informer := range i.clusterNameToInformer {
		informer.AddEventHandler(handler)
	}
}

// AddEventHandlerWithResyncPeriod adds the handler with a resync period to each cluster scoped informer.
func (i *multiClusterInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) {
	for _, informer := range i.clusterNameToInformer {
		informer.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	}
}

// AddIndexers adds the indexer for each cluster scoped informer.
func (i *multiClusterInformer) AddIndexers(indexers toolscache.Indexers) error {
	for _, informer := range i.clusterNameToInformer {
		err := informer.AddIndexers(indexers)
		if err != nil {
			return err
		}
	}
	return nil
}

// HasSynced checks if each namespaced informer has synced.
func (i *multiClusterInformer) HasSynced() bool {
	for _, informer := range i.clusterNameToInformer {
		if ok := informer.HasSynced(); !ok {
			return ok
		}
	}
	return true
}
