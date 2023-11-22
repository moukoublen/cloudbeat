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
	"errors"

	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/samber/lo"

	"github.com/elastic/cloudbeat/resources/fetching"
	"github.com/elastic/cloudbeat/resources/providers/azurelib/inventory"
)

type AzureInsightsBatchAssetFetcher struct {
	log        *logp.Logger
	resourceCh chan fetching.ResourceInfo
	provider   inventory.ServiceAPI
}

func NewAzureInsightsBatchAssetFetcher(log *logp.Logger, ch chan fetching.ResourceInfo, provider inventory.ServiceAPI) *AzureInsightsBatchAssetFetcher {
	return &AzureInsightsBatchAssetFetcher{
		log:        log,
		resourceCh: ch,
		provider:   provider,
	}
}

func (f *AzureInsightsBatchAssetFetcher) Fetch(ctx context.Context, cMetadata fetching.CycleMetadata) error {
	f.log.Info("Starting AzureInsightsBatchAssetFetcher.Fetch")
	subscriptions := f.provider.GetSubscriptions()

	assets, err := f.provider.ListDiagnosticSettingsAssetTypes(ctx)
	if err != nil {
		f.log.Errorf("AzureInsightsBatchAssetFetcher.Fetch failed to fetch diagnostic settings: %s", err.Error())
		return err
	}

	// group and send by subscription id
	subscriptionGroups := lo.GroupBy(assets, func(item inventory.AzureAsset) string {
		return item.SubscriptionId
	})

	var errAgg error
	for subId, subName := range subscriptions {
		batchAssets := subscriptionGroups[subId]
		if batchAssets == nil {
			batchAssets = []inventory.AzureAsset{} // Use empty array instead of nil
		}

		select {
		case <-ctx.Done():
			err := ctx.Err()
			f.log.Infof("AzureInsightsBatchAssetFetcher.Fetch context err: %s", err.Error())
			errAgg = errors.Join(errAgg, err)
			return errAgg
		case f.resourceCh <- fetching.ResourceInfo{
			CycleMetadata: cMetadata,
			Resource: &AzureBatchResource{
				Type:    fetching.MonitoringIdentity,
				SubType: fetching.AzureDiagnosticSettingsType,
				SubId:   subId,
				SubName: subName,
				Assets:  batchAssets,
			},
		}:
		}
	}

	return errAgg
}

func (f *AzureInsightsBatchAssetFetcher) Stop() {}
