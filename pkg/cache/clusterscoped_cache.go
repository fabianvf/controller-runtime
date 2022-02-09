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

// a new global namespaced cache to handle cluster scoped resources.
const globalClusterCache = "_cluster"

// [_cluster -> cache ("*"), clusterNames -> caches ]

// MultiNamespacedCacheBuilder - Builder function to create a new multi-namespaced cache.
// This will scope the cache to a list of namespaces. Listing for all namespaces
// will list for all the namespaces that this knows about. By default this will create
// a global cache for cluster scoped resource. Note that this is not intended
// to be used for excluding namespaces, this is better done via a Predicate. Also note that
// you may face performance issues when using this with a high number of namespaces.
func MultiClusterCacheBuilder(clusterNames []string) NewCacheFunc {
	return func(config *rest.Config, opts Options) (Cache, error) {
		opts, err := defaultOpts(config, opts)
		if err != nil {
			return nil, err
		}

		caches := map[string]Cache{}

		// create aglobal cache for * scope
		gCache, err := New(config, opts)
		if err != nil {
			return nil, fmt.Errorf("error creating global cache %v", err)
		}

		for _, cs := range clusterNames {
			opts.ClusterName = cs
			c, err := New(config, opts)
			if err != nil {
				return nil, err
			}
			caches[cs] = c
		}
		return &multiClusterCache{clusterToCache: caches, Scheme: opts.Scheme, RESTMapper: opts.Mapper, gClusterCache: gCache}, nil
	}
}

// multiNamespaceCache knows how to handle multiple namespaced caches
// Use this feature when scoping permissions for your
// operator to a list of namespaces instead of watching every namespace
// in the cluster.
type multiClusterCache struct {
	clusterToCache map[string]Cache
	Scheme         *runtime.Scheme
	RESTMapper     apimeta.RESTMapper
	gClusterCache  Cache // Point to "*"
}

var _ Cache = &multiClusterCache{}

// Methods for multiNamespaceCache to conform to the Informers interface.
func (c *multiClusterCache) GetInformer(ctx context.Context, obj client.Object) (Informer, error) {
	informers := map[string]Informer{}

	//get CLusterName
	clusterName, err := getClusterName(obj)
	if err != nil {
		return nil, fmt.Errorf("error getting clustername %q", err)
	}

	if (clusterName) == "*" {
		globalInformer, err := c.gClusterCache.GetInformer(ctx, obj)
		if err != nil {
			return nil, err
		}
		informers[globalClusterCache] = globalInformer
	}

	for cs, cache := range c.clusterToCache {
		informer, err := cache.GetInformer(ctx, obj)
		if err != nil {
			return nil, err
		}
		informers[cs] = informer
	}

	return &multiClusterInformer{clusterNameToInformer: informers}, nil

}

func getClusterName(obj client.Object) (string, error) {
	if obj == nil {
		return "", fmt.Errorf("object cannot be empty %v", obj)
	}
	if obj.GetClusterName() != "" {
		return "*", nil
	}

	return obj.GetClusterName(), nil
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

	clusterName, err := getClusterName(obj)
	if err != nil {
		return err
	}

	if clusterName == "*" {
		return c.gClusterCache.IndexField(ctx, obj, field, extractValue)
	}

	for _, cache := range c.clusterToCache {
		if err := cache.IndexField(ctx, obj, field, extractValue); err != nil {
			return err
		}
	}
	return nil
}

func (c *multiClusterCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	clusterName, err := getClusterName(obj)
	if err != nil {
		return err
	}

	if clusterName == "*" {
		// Look into the global cache to fetch the object
		return c.gClusterCache.Get(ctx, key, obj)
	}

	cache, ok := c.clusterToCache[clusterName]
	if !ok {
		return fmt.Errorf("unable to get: %v because of unknown clusterName for the cache", key)
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
		// initial stab - error out
		fmt.Errorf("cluster Name is empty in listOpts %v", listOpts)
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

// AddEventHandler adds the handler to each namespaced informer.
func (i *multiClusterInformer) AddEventHandler(handler toolscache.ResourceEventHandler) {
	for _, informer := range i.clusterNameToInformer {
		informer.AddEventHandler(handler)
	}
}

// AddEventHandlerWithResyncPeriod adds the handler with a resync period to each namespaced informer.
func (i *multiClusterInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) {
	for _, informer := range i.clusterNameToInformer {
		informer.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	}
}

// AddIndexers adds the indexer for each namespaced informer.
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
