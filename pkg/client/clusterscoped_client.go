/*
Copyright 2014 The Kubernetes Authors.

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

package client

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

var _ Reader = &clusterScopedClient{}
var _ Writer = &clusterScopedClient{}
var _ StatusWriter = &clusterScopedClient{}

// client is a client.Client that reads and writes directly from/to an API server.  It lazily initializes
// new clients at the time they are used, and caches the client.
type clusterScopedClient struct {
	cache      *clientCache
	paramCodec runtime.ParameterCodec
}

func (cc *clusterScopedClient) Create(ctx context.Context, obj Object, opts ...CreateOption) error {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("unstructured client did not understand object: %T", obj)
	}

	gvk := u.GroupVersionKind()

	o, err := cc.cache.getObjMeta(obj)
	if err != nil {
		return err
	}

	createOpts := &CreateOptions{}
	createOpts.ApplyOptions(opts)

	result := o.Post().
		Cluster(obj.GetClusterName()).
		NamespaceIfScoped(o.GetNamespace(), o.isNamespaced()).
		Resource(o.resource()).
		Body(obj).
		VersionedParams(createOpts.AsCreateOptions(), cc.paramCodec).
		Do(ctx).
		Into(obj)

	u.SetGroupVersionKind(gvk)
	return result
}

func (cc *clusterScopedClient) Update(ctx context.Context, obj Object, opts ...UpdateOption) error {
	return nil
}

func (cc *clusterScopedClient) Delete(ctx context.Context, obj Object, opts ...DeleteOption) error {
	return nil
}

func (cc *clusterScopedClient) DeleteAllOf(ctx context.Context, obj Object, opts ...DeleteAllOfOption) error {
	return nil
}

// Patch implements client.Client.
func (cc *clusterScopedClient) Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error {
	return nil
}

func (cc *clusterScopedClient) Get(ctx context.Context, key ObjectKey, obj Object) error {
	return nil
}

func (cc *clusterScopedClient) List(ctx context.Context, obj ObjectList, opts ...ListOption) error {
	return nil
}

func (cc *clusterScopedClient) UpdateStatus(ctx context.Context, obj Object, opts ...UpdateOption) error {
	return nil
}

func (cc *clusterScopedClient) PatchStatus(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error {
	return nil
}
