package eventgrid

import (
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/preview/eventgrid/mgmt/2020-04-01-preview/eventgrid"
	"github.com/hashicorp/go-azure-helpers/response"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/clients"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/eventgrid/parse"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/tags"
	azSchema "github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/tf/schema"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/timeouts"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceArmEventGridSystemTopic() *schema.Resource {
	return &schema.Resource{
		Create: resourceArmEventGridSystemTopicCreateUpdate,
		Read:   resourceArmEventGridSystemTopicRead,
		Update: resourceArmEventGridSystemTopicCreateUpdate,
		Delete: resourceArmEventGridSystemTopicDelete,

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(30 * time.Minute),
			Read:   schema.DefaultTimeout(5 * time.Minute),
			Update: schema.DefaultTimeout(30 * time.Minute),
			Delete: schema.DefaultTimeout(30 * time.Minute),
		},

		Importer: azSchema.ValidateResourceIDPriorToImport(func(id string) error {
			_, err := parse.EventGridSystemTopicID(id)
			return err
		}),

		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"location": azure.SchemaLocation(),

			"resource_group_name": azure.SchemaResourceGroupName(),

			"source_resource_id": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: azure.ValidateResourceID,
			},

			"topic_type": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				ValidateFunc: validation.StringInSlice([]string{
					"Microsoft.AppConfiguration.ConfigurationStores",
					"Microsoft.Communication.CommunicationServices",
					"Microsoft.ContainerRegistry.Registries",
					"Microsoft.Devices.IoTHubs",
					"Microsoft.EventGrid.Domains",
					"Microsoft.EventGrid.Topics",
					"Microsoft.Eventhub.Namespaces",
					"Microsoft.KeyVault.vaults",
					"Microsoft.MachineLearningServices.Workspaces",
					"Microsoft.Maps.Accounts",
					"Microsoft.Media.MediaServices",
					"Microsoft.Resources.ResourceGroups",
					"Microsoft.Resources.Subscriptions",
					"Microsoft.ServiceBus.Namespaces",
					"Microsoft.SignalRService.SignalR",
					"Microsoft.Storage.StorageAccounts",
					"Microsoft.Web.ServerFarms",
					"Microsoft.Web.Sites",
				}, false),
			},

			"metric_resource_id": {
				Type:     schema.TypeString,
				Computed: true,
			},

			"tags": tags.Schema(),
		},
	}
}

func resourceArmEventGridSystemTopicCreateUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).EventGrid.SystemTopicsClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	name := d.Get("name").(string)
	resourceGroup := d.Get("resource_group_name").(string)
	source := d.Get("source_resource_id").(string)
	topicType := d.Get("topic_type").(string)

	if d.IsNewResource() {
		existing, err := client.Get(ctx, resourceGroup, name)
		if err != nil {
			if !utils.ResponseWasNotFound(existing.Response) {
				return fmt.Errorf("Error checking for presence of existing EventGrid System Topic %q (Resource Group %q): %s", name, resourceGroup, err)
			}
		}

		if existing.ID != nil && *existing.ID != "" {
			return tf.ImportAsExistsError("azurerm_eventgrid_system_topic", *existing.ID)
		}
	}

	location := azure.NormalizeLocation(d.Get("location").(string))
	t := d.Get("tags").(map[string]interface{})

	systemTopic := eventgrid.SystemTopic{
		Location: &location,
		SystemTopicProperties: &eventgrid.SystemTopicProperties{
			Source:    &source,
			TopicType: &topicType,
		},
		Tags: tags.Expand(t),
	}

	log.Printf("[INFO] preparing arguments for AzureRM EventGrid System Topic creation with Properties: %+v.", systemTopic)

	future, err := client.CreateOrUpdate(ctx, resourceGroup, name, systemTopic)
	if err != nil {
		return err
	}

	if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
		return err
	}

	read, err := client.Get(ctx, resourceGroup, name)
	if err != nil {
		return err
	}
	if read.ID == nil {
		return fmt.Errorf("Cannot read EventGrid System Topic %s (resource group %s) ID", name, resourceGroup)
	}

	d.SetId(*read.ID)

	return resourceArmEventGridSystemTopicRead(d, meta)
}

func resourceArmEventGridSystemTopicRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).EventGrid.SystemTopicsClient
	ctx, cancel := timeouts.ForRead(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.EventGridSystemTopicID(d.Id())
	if err != nil {
		return err
	}

	resp, err := client.Get(ctx, id.ResourceGroup, id.Name)
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			log.Printf("[WARN] EventGrid System Topic '%s' was not found (resource group '%s')", id.Name, id.ResourceGroup)
			d.SetId("")
			return nil
		}

		return fmt.Errorf("Error making Read request on EventGrid System Topic '%s': %+v", id.Name, err)
	}

	d.Set("name", resp.Name)
	d.Set("resource_group_name", id.ResourceGroup)
	if location := resp.Location; location != nil {
		d.Set("location", azure.NormalizeLocation(*location))
	}

	if props := resp.SystemTopicProperties; props != nil {
		d.Set("source_resource_id", props.Source)
		d.Set("topic_type", props.TopicType)
		d.Set("metric_resource_id", props.MetricResourceID)
	}

	return tags.FlattenAndSet(d, resp.Tags)
}

func resourceArmEventGridSystemTopicDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).EventGrid.SystemTopicsClient
	ctx, cancel := timeouts.ForDelete(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.EventGridSystemTopicID(d.Id())
	if err != nil {
		return err
	}

	future, err := client.Delete(ctx, id.ResourceGroup, id.Name)
	if err != nil {
		if response.WasNotFound(future.Response()) {
			return nil
		}
		return fmt.Errorf("Error deleting EventGrid System Topic %q: %+v", id.Name, err)
	}

	if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
		if response.WasNotFound(future.Response()) {
			return nil
		}
		return fmt.Errorf("Error deleting EventGrid System Topic %q: %+v", id.Name, err)
	}

	return nil
}
