package sql

// Copyright (c) Microsoft and contributors.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Code generated by Microsoft (R) AutoRest Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.

import (
	"context"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/tracing"
	"net/http"
)

// SubscriptionUsagesClient is the the Azure SQL Database management API provides a RESTful set of web services that
// interact with Azure SQL Database services to manage your databases. The API enables you to create, retrieve, update,
// and delete databases.
type SubscriptionUsagesClient struct {
	BaseClient
}

// NewSubscriptionUsagesClient creates an instance of the SubscriptionUsagesClient client.
func NewSubscriptionUsagesClient(subscriptionID string) SubscriptionUsagesClient {
	return NewSubscriptionUsagesClientWithBaseURI(DefaultBaseURI, subscriptionID)
}

// NewSubscriptionUsagesClientWithBaseURI creates an instance of the SubscriptionUsagesClient client using a custom
// endpoint.  Use this when interacting with an Azure cloud that uses a non-standard base URI (sovereign clouds, Azure
// stack).
func NewSubscriptionUsagesClientWithBaseURI(baseURI string, subscriptionID string) SubscriptionUsagesClient {
	return SubscriptionUsagesClient{NewWithBaseURI(baseURI, subscriptionID)}
}

// Get gets a subscription usage metric.
// Parameters:
// locationName - the name of the region where the resource is located.
// usageName - name of usage metric to return.
func (client SubscriptionUsagesClient) Get(ctx context.Context, locationName string, usageName string) (result SubscriptionUsage, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/SubscriptionUsagesClient.Get")
		defer func() {
			sc := -1
			if result.Response.Response != nil {
				sc = result.Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	req, err := client.GetPreparer(ctx, locationName, usageName)
	if err != nil {
		err = autorest.NewErrorWithError(err, "sql.SubscriptionUsagesClient", "Get", nil, "Failure preparing request")
		return
	}

	resp, err := client.GetSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "sql.SubscriptionUsagesClient", "Get", resp, "Failure sending request")
		return
	}

	result, err = client.GetResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "sql.SubscriptionUsagesClient", "Get", resp, "Failure responding to request")
		return
	}

	return
}

// GetPreparer prepares the Get request.
func (client SubscriptionUsagesClient) GetPreparer(ctx context.Context, locationName string, usageName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"locationName":   autorest.Encode("path", locationName),
		"subscriptionId": autorest.Encode("path", client.SubscriptionID),
		"usageName":      autorest.Encode("path", usageName),
	}

	const APIVersion = "2015-05-01-preview"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/providers/Microsoft.Sql/locations/{locationName}/usages/{usageName}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// GetSender sends the Get request. The method will close the
// http.Response Body if it receives an error.
func (client SubscriptionUsagesClient) GetSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, azure.DoRetryWithRegistration(client.Client))
}

// GetResponder handles the response to the Get request. The method always
// closes the http.Response Body.
func (client SubscriptionUsagesClient) GetResponder(resp *http.Response) (result SubscriptionUsage, err error) {
	err = autorest.Respond(
		resp,
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// ListByLocation gets all subscription usage metrics in a given location.
// Parameters:
// locationName - the name of the region where the resource is located.
func (client SubscriptionUsagesClient) ListByLocation(ctx context.Context, locationName string) (result SubscriptionUsageListResultPage, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/SubscriptionUsagesClient.ListByLocation")
		defer func() {
			sc := -1
			if result.sulr.Response.Response != nil {
				sc = result.sulr.Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	result.fn = client.listByLocationNextResults
	req, err := client.ListByLocationPreparer(ctx, locationName)
	if err != nil {
		err = autorest.NewErrorWithError(err, "sql.SubscriptionUsagesClient", "ListByLocation", nil, "Failure preparing request")
		return
	}

	resp, err := client.ListByLocationSender(req)
	if err != nil {
		result.sulr.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "sql.SubscriptionUsagesClient", "ListByLocation", resp, "Failure sending request")
		return
	}

	result.sulr, err = client.ListByLocationResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "sql.SubscriptionUsagesClient", "ListByLocation", resp, "Failure responding to request")
		return
	}
	if result.sulr.hasNextLink() && result.sulr.IsEmpty() {
		err = result.NextWithContext(ctx)
	}

	return
}

// ListByLocationPreparer prepares the ListByLocation request.
func (client SubscriptionUsagesClient) ListByLocationPreparer(ctx context.Context, locationName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"locationName":   autorest.Encode("path", locationName),
		"subscriptionId": autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2015-05-01-preview"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/providers/Microsoft.Sql/locations/{locationName}/usages", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ListByLocationSender sends the ListByLocation request. The method will close the
// http.Response Body if it receives an error.
func (client SubscriptionUsagesClient) ListByLocationSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, azure.DoRetryWithRegistration(client.Client))
}

// ListByLocationResponder handles the response to the ListByLocation request. The method always
// closes the http.Response Body.
func (client SubscriptionUsagesClient) ListByLocationResponder(resp *http.Response) (result SubscriptionUsageListResult, err error) {
	err = autorest.Respond(
		resp,
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// listByLocationNextResults retrieves the next set of results, if any.
func (client SubscriptionUsagesClient) listByLocationNextResults(ctx context.Context, lastResults SubscriptionUsageListResult) (result SubscriptionUsageListResult, err error) {
	req, err := lastResults.subscriptionUsageListResultPreparer(ctx)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "sql.SubscriptionUsagesClient", "listByLocationNextResults", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}
	resp, err := client.ListByLocationSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "sql.SubscriptionUsagesClient", "listByLocationNextResults", resp, "Failure sending next results request")
	}
	result, err = client.ListByLocationResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "sql.SubscriptionUsagesClient", "listByLocationNextResults", resp, "Failure responding to next results request")
		return
	}
	return
}

// ListByLocationComplete enumerates all values, automatically crossing page boundaries as required.
func (client SubscriptionUsagesClient) ListByLocationComplete(ctx context.Context, locationName string) (result SubscriptionUsageListResultIterator, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/SubscriptionUsagesClient.ListByLocation")
		defer func() {
			sc := -1
			if result.Response().Response.Response != nil {
				sc = result.page.Response().Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	result.page, err = client.ListByLocation(ctx, locationName)
	return
}
