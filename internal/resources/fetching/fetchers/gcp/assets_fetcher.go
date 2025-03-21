// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package fetchers

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"cloud.google.com/go/asset/apiv1/assetpb"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/elastic/cloudbeat/internal/resources/fetching"
	"github.com/elastic/cloudbeat/internal/resources/fetching/cycle"
	"github.com/elastic/cloudbeat/internal/resources/providers/gcplib/inventory"
)

var (
	prepareWG = sync.WaitGroup{}
	start     = make(chan struct{}, 10)
)

func init() {
	prepareWG.Add(2)
	go func() {
		prepareWG.Wait()
		close(start)
	}()
}

const (
	subLen      = 1000
	mMultiplier = 900
)

var multipliers = map[string]int{
	fetching.ProjectManagement: 1,
	fetching.KeyManagement:     1,
	fetching.CloudIdentity:     1,
	fetching.CloudDatabase:     1,
	fetching.CloudStorage:      1,
	fetching.CloudCompute:      1,
	fetching.CloudDns:          1,
	fetching.DataProcessing:    1,
}

type GcpAssetsFetcher struct {
	log        *logp.Logger
	resourceCh chan fetching.ResourceInfo
	provider   inventory.ServiceAPI
}

type GcpAsset struct {
	Type    string
	SubType string

	ExtendedAsset *inventory.ExtendedGcpAsset `json:"asset,omitempty"`
}

// GcpAssetTypes https://cloud.google.com/asset-inventory/docs/supported-asset-types
// map of types to asset types.
// sub-type is derived from asset type by using the first and last segments of the asset type name
// example: gcp-cloudkms-crypto-key
var GcpAssetTypes = map[string][]string{
	fetching.ProjectManagement: {
		inventory.CrmProjectAssetType,
	},
	fetching.KeyManagement: {
		inventory.ApiKeysKeyAssetType,
		inventory.CloudKmsCryptoKeyAssetType,
	},
	fetching.CloudIdentity: {
		inventory.IamServiceAccountAssetType,
		inventory.IamServiceAccountKeyAssetType,
	},
	fetching.CloudDatabase: {
		inventory.BigqueryDatasetAssetType,
		inventory.BigqueryTableAssetType,
		inventory.SqlDatabaseInstanceAssetType,
	},
	fetching.CloudStorage: {
		inventory.StorageBucketAssetType,
		inventory.LogBucketAssetType,
	},
	fetching.CloudCompute: {
		inventory.ComputeInstanceAssetType,
		inventory.ComputeFirewallAssetType,
		inventory.ComputeDiskAssetType,
		inventory.ComputeNetworkAssetType,
		inventory.ComputeBackendServiceAssetType,
		inventory.ComputeSubnetworkAssetType,
	},
	fetching.CloudDns: {
		inventory.DnsManagedZoneAssetType,
	},
	fetching.DataProcessing: {
		inventory.DataprocClusterAssetType,
	},
}

func NewGcpAssetsFetcher(_ context.Context, log *logp.Logger, ch chan fetching.ResourceInfo, provider inventory.ServiceAPI) *GcpAssetsFetcher {
	return &GcpAssetsFetcher{
		log:        log,
		resourceCh: ch,
		provider:   provider,
	}
}

func (f *GcpAssetsFetcher) Fetch(ctx context.Context, cycleMetadata cycle.Metadata) error {
	return nil
	f.log.Info("Starting GcpAssetsFetcher.Fetch")

	for typeName, assetTypes := range GcpAssetTypes {
		assets, err := f.provider.ListAllAssetTypesByName(ctx, assetTypes)
		if err != nil {
			f.log.Errorf("Failed to list assets for type %s: %s", typeName, err.Error())
			continue
		}

		mm := multipliers[typeName]
		mult := make([]*inventory.ExtendedGcpAsset, 0, len(assets)*mm)
		for _, asset := range assets {
			for range mm {
				mult = append(mult, cloneExtendedGcpAsset(asset))
			}
		}
		f.log.Errorf(">>> GcpAssetsFetcher %s: %d", typeName, len(mult))

		for _, asset := range mult {
			select {
			case <-ctx.Done():
				f.log.Infof("GcpAssetsFetcher.Fetch context err: %s", ctx.Err().Error())
				return nil
			case f.resourceCh <- fetching.ResourceInfo{
				CycleMetadata: cycleMetadata,
				Resource: &GcpAsset{
					Type:          typeName,
					SubType:       getGcpSubType(asset.AssetType),
					ExtendedAsset: asset,
				},
			}:
			}
		}
	}

	return nil
}

func (f *GcpAssetsFetcher) Stop() {
	f.provider.Close()
}

func (r *GcpAsset) GetData() any {
	return r.ExtendedAsset.Asset
}

func (r *GcpAsset) GetIds() []string {
	return []string{r.ExtendedAsset.Name}
}

func (r *GcpAsset) GetMetadata() (fetching.ResourceMetadata, error) {
	var region string

	if r.ExtendedAsset.Resource != nil {
		region = r.ExtendedAsset.Resource.Location
	}

	return fetching.ResourceMetadata{
		ID:                   r.ExtendedAsset.Name,
		Type:                 r.Type,
		SubType:              r.SubType,
		Name:                 getAssetResourceName(r.ExtendedAsset),
		Region:               region,
		CloudAccountMetadata: *r.ExtendedAsset.CloudAccount,
	}, nil
}

func (r *GcpAsset) GetElasticCommonData() (map[string]any, error) {
	m := map[string]any{}

	if r.Type == fetching.CloudIdentity {
		m["user.effective.id"] = r.ExtendedAsset.Name
		m["user.effective.name"] = getAssetResourceName(r.ExtendedAsset)
	}

	if r.Type == fetching.CloudCompute && r.ExtendedAsset.AssetType == inventory.ComputeInstanceAssetType {
		fields := getAssetDataFields(r.ExtendedAsset)
		if fields == nil {
			return m, nil
		}
		nameField, ok := fields["name"]
		if ok {
			if name := nameField.GetStringValue(); name != "" {
				m["host.name"] = name
			}
		}
		hostnameField, ok := fields["hostname"]
		if ok {
			if hostname := hostnameField.GetStringValue(); hostname != "" {
				m["host.hostname"] = hostname
			}
		}
	}

	return m, nil
}

// try to retrieve the resource name from the asset data fields (name or displayName), in case it is not set
// get the last part of the asset name (https://cloud.google.com/apis/design/resource_names#resource_id)
func getAssetResourceName(asset *inventory.ExtendedGcpAsset) string {
	fields := getAssetDataFields(asset)
	if fields != nil {
		if name, exist := fields["displayName"]; exist && name.GetStringValue() != "" {
			return name.GetStringValue()
		}

		if name, exist := fields["name"]; exist && name.GetStringValue() != "" {
			return name.GetStringValue()
		}
	}
	parts := strings.Split(asset.Name, "/")
	return parts[len(parts)-1]
}

func getGcpSubType(assetType string) string {
	dotIndex := strings.Index(assetType, ".")
	slashIndex := strings.Index(assetType, "/")

	prefix := assetType[:dotIndex]
	suffix := assetType[slashIndex+1:]

	return strings.ToLower(fmt.Sprintf("gcp-%s-%s", prefix, xstrings.ToKebabCase(suffix)))
}

// getAssetDataFields tries to retrieve asset.resource.data fields if possible.
// Returns nil otherwise.
func getAssetDataFields(asset *inventory.ExtendedGcpAsset) map[string]*structpb.Value {
	resource := asset.GetResource()
	if resource == nil {
		return nil
	}
	data := resource.GetData()
	if data == nil {
		return nil
	}
	return data.GetFields()
}

func cloneExtendedGcpAsset(e *inventory.ExtendedGcpAsset) *inventory.ExtendedGcpAsset {
	n := &inventory.ExtendedGcpAsset{}
	n.CloudAccount = e.CloudAccount
	n.Asset = proto.Clone(e.Asset).(*assetpb.Asset)
	n.Asset.Name += "_" + randString(10)
	return n
}

func cloneExtendedGcpAssets(e []*inventory.ExtendedGcpAsset, subLen int) []*inventory.ExtendedGcpAsset {
	if len(e) > subLen {
		return e
	}

	sl := make([]*inventory.ExtendedGcpAsset, 0, subLen)
	l := len(e)
	for i := range subLen {
		a := e[i%l]
		sl = append(sl, cloneExtendedGcpAsset(a))
	}
	return sl
}

func cloneServiceUsageAsset(e *inventory.ServiceUsageAsset) *inventory.ServiceUsageAsset {
	n := &inventory.ServiceUsageAsset{}
	n.CloudAccount = e.CloudAccount
	n.Services = cloneExtendedGcpAssets(e.Services, subLen)
	return n
}

func cloneProjectPoliciesAsset(e *inventory.ProjectPoliciesAsset) *inventory.ProjectPoliciesAsset {
	n := &inventory.ProjectPoliciesAsset{}
	n.CloudAccount = e.CloudAccount
	n.Policies = cloneExtendedGcpAssets(e.Policies, subLen)
	return n
}

func cloneLoggingAsset(e *inventory.LoggingAsset) *inventory.LoggingAsset {
	n := &inventory.LoggingAsset{}
	n.CloudAccount = e.CloudAccount
	n.LogSinks = cloneExtendedGcpAssets(e.LogSinks, subLen)
	return n
}

func cloneMonitoringAsset(e *inventory.MonitoringAsset) *inventory.MonitoringAsset {
	n := &inventory.MonitoringAsset{}
	n.CloudAccount = e.CloudAccount
	n.LogMetrics = cloneExtendedGcpAssets(e.LogMetrics, subLen)
	n.Alerts = cloneExtendedGcpAssets(e.Alerts, subLen)
	return n
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
