package kusto

import (
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/clients"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/kusto/parse"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/timeouts"
)

type MappingRec struct {
	Name     string
	Kind     string
	Mapping  string
	Database string
	Table    string
}

func resourceArmKustoDatabaseTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceArmKustoDatabaseTableCreateUpdate,
		Read:   resourceArmKustoDatabaseTableRead,
		Update: resourceArmKustoDatabaseTableCreateUpdate,
		Delete: resourceArmKustoDatabaseTableDelete,

		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(60 * time.Minute),
			Read:   schema.DefaultTimeout(5 * time.Minute),
			Update: schema.DefaultTimeout(60 * time.Minute),
			Delete: schema.DefaultTimeout(60 * time.Minute),
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validateAzureRMKustoClusterName,
			},

			"resource_group_name": azure.SchemaResourceGroupName(),

			"cluster_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validateAzureRMKustoClusterName,
			},

			"database_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validateAzureRMKustoDatabaseName,
			},

			"table_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"kind": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				ValidateFunc: validation.StringInSlice([]string{
					"csv",
					"json",
					"avro",
					"parquet",
					"orc",
				}, true),
			},

			"mapping": {
				Type:     schema.TypeList,
				MinItems: 1,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"column": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringIsNotEmpty,
						},
						"data_type": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"properties": {
							Type:     schema.TypeList,
							MinItems: 1,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"field": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.StringIsNotEmpty,
									},
									"path": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.StringIsNotEmpty,
									},
									"ordinal": {
										Type:     schema.TypeInt,
										Optional: true,
									},
									"constant_value": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.StringIsNotEmpty,
									},
									"transform": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.StringIsNotEmpty,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func resourceArmKustoDatabaseTableCreateUpdate(d *schema.ResourceData, meta interface{}) error {
	clientCredentialsConfig := meta.(*clients.Client).Kusto.ClientCredentialsConfig
	clusterClient := meta.(*clients.Client).Kusto.ClustersClient
	databaseClient := meta.(*clients.Client).Kusto.DatabasesClient

	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	log.Printf("[INFO] preparing arguments for Azure Kusto Database Table creation.")

	name := d.Get("name").(string)
	resourceGroupName := d.Get("resource_group_name").(string)
	clusterName := d.Get("cluster_name").(string)
	databaseName := d.Get("database_name").(string)
	tableName := d.Get("table_name").(string)
	kind := d.Get("kind").(string)
	mapping := d.Get("mapping").(interface{})

	cluster, err := clusterClient.Get(ctx, resourceGroupName, clusterName)
	if err != nil {
		return fmt.Errorf("Error reading Kusto Cluster %q: %+v", clusterName, err)
	}
	database, err := databaseClient.Get(ctx, resourceGroupName, clusterName, databaseName)
	if err != nil {
		return fmt.Errorf("Error reading Kusto Database %q: %+v", databaseName, err)
	}

	authorizer := kusto.Authorization{
		Config: clientCredentialsConfig,
	}
	client, err := kusto.New(*cluster.URI, authorizer)

	if err != nil {
		return fmt.Errorf("Error creating Kusto Cluster client %s: %+v", clusterName, err)
	}

	readWriteDB, _ := database.Value.AsReadWriteDatabase()
	resourceID := fmt.Sprintf("%s/Tables/%s/Mappings/%s/%s", *readWriteDB.ID, tableName, kind, name)

	if d.IsNewResource() {
		stmtRaw := fmt.Sprintf(".show table %s ingestion %s mappings | where Name == \"%s\"", tableName, kind, name)

		stmt := kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(stmtRaw)
		iter, err := client.Mgmt(ctx, databaseName, stmt)
		if err != nil {
			return fmt.Errorf("Error querying Kusto: %+v", err)
		}
		defer iter.Stop()

		recs := []MappingRec{}
		err = iter.Do(
			func(row *table.Row) error {
				rec := MappingRec{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs = append(recs, rec)
				return nil
			},
		)

		if len(recs) > 0 {
			return tf.ImportAsExistsError("azurerm_kusto_database_table_mapping", resourceID)
		}
	}

	// TODO: expand column mappings
	columnMappings := ""
	stmtRaw := fmt.Sprintf(".create-or-alter table %s ingestion %s mapping \"%s\" '%s'", tableName, kind, name, columnMappings)

	stmt := kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(stmtRaw)
	iter, err := client.Mgmt(ctx, databaseName, stmt)
	if err != nil {
		return fmt.Errorf("Error querying Kusto: %+v", err)
	}
	defer iter.Stop()

	err = iter.Do(
		func(row *table.Row) error {
			return nil
		},
	)

	d.SetId(resourceID)

	return resourceArmKustoDatabaseTableRead(d, meta)
}

func resourceArmKustoDatabaseTableRead(d *schema.ResourceData, meta interface{}) error {
	clientCredentialsConfig := meta.(*clients.Client).Kusto.ClientCredentialsConfig
	clusterClient := meta.(*clients.Client).Kusto.ClustersClient
	databaseClient := meta.(*clients.Client).Kusto.DatabasesClient

	ctx, cancel := timeouts.ForRead(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.KustoDatabaseTableMappingID(d.Id())
	if err != nil {
		return err
	}

	cluster, err := clusterClient.Get(ctx, id.ResourceGroup, id.Cluster)
	if err != nil {
		return fmt.Errorf("Error reading Kusto Cluster %q (Resource Group %q): %+v", id.Cluster, id.ResourceGroup, err)
	}
	_, err = databaseClient.Get(ctx, id.ResourceGroup, id.Cluster, id.Database)
	if err != nil {
		return fmt.Errorf("Error reading Kusto Database %q (Resource Group %q, Cluster %q): %+v", id.Database, id.ResourceGroup, id.Cluster, err)
	}

	authorizer := kusto.Authorization{
		Config: clientCredentialsConfig,
	}
	client, err := kusto.New(*cluster.URI, authorizer)
	if err != nil {
		return fmt.Errorf("Error creating Kusto Cluster client %s: %+v", *cluster.URI, err)
	}

	stmtRaw := fmt.Sprintf(".show table %s ingestion %s mappings | where Name == \"%s\"", id.Table, id.Kind, id.Name)

	stmt := kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(stmtRaw)
	iter, err := client.Mgmt(ctx, id.Database, stmt)
	if err != nil {
		return fmt.Errorf("Error querying Kusto: %+v", err)
	}
	defer iter.Stop()

	recs := []MappingRec{}
	err = iter.Do(
		func(row *table.Row) error {
			rec := MappingRec{}
			if err := row.ToStruct(&rec); err != nil {
				return err
			}
			recs = append(recs, rec)
			return nil
		},
	)

	if len(recs) == 0 {
		d.SetId("")
		return nil
	}

	d.Set("resource_group_name", id.ResourceGroup)
	d.Set("cluster_name", id.Cluster)
	d.Set("database_name", recs[0].Database)
	d.Set("table_name", recs[0].Table)
	d.Set("name", recs[0].Name)
	d.Set("kind", recs[0].Kind)
	d.Set("mapping", flattenKustoTableMapping(recs[0].Mapping))

	return nil
}

func resourceArmKustoDatabaseTableDelete(d *schema.ResourceData, meta interface{}) error {
	clientCredentialsConfig := meta.(*clients.Client).Kusto.ClientCredentialsConfig
	clusterClient := meta.(*clients.Client).Kusto.ClustersClient
	databaseClient := meta.(*clients.Client).Kusto.DatabasesClient

	ctx, cancel := timeouts.ForDelete(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.KustoDatabaseTableMappingID(d.Id())
	if err != nil {
		return err
	}

	cluster, err := clusterClient.Get(ctx, id.ResourceGroup, id.Cluster)
	if err != nil {
		return fmt.Errorf("Error reading Kusto Cluster %q (Resource Group %q): %+v", id.Cluster, id.ResourceGroup, err)
	}
	_, err = databaseClient.Get(ctx, id.ResourceGroup, id.Cluster, id.Database)
	if err != nil {
		return fmt.Errorf("Error reading Kusto Database %q (Resource Group %q, Cluster %q): %+v", id.Database, id.ResourceGroup, id.Cluster, err)
	}

	authorizer := kusto.Authorization{
		Config: clientCredentialsConfig,
	}
	client, err := kusto.New(*cluster.URI, authorizer)
	if err != nil {
		return fmt.Errorf("Error creating Kusto Cluster client %s: %+v", *cluster.URI, err)
	}

	stmtRaw := fmt.Sprintf(".drop table %s ingestion %s mapping %s", id.Table, id.Kind, id.Name)
	stmt := kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(stmtRaw)
	iter, err := client.Mgmt(ctx, id.Database, stmt)
	if err != nil {
		return fmt.Errorf("Error deleting Kusto Database Table Ingestion Mapping %q (Cluster %q, Database %q, Table %q): %+v", id.Name, id.Cluster, id.Database, id.Table, err)
	}
	defer iter.Stop()

	err = iter.Do(
		func(row *table.Row) error {
			return nil
		},
	)

	return nil
}

func flattenKustoTableMapping(mapping string) []interface{} {
	if len(mapping) == 0 {
		return nil
	}

	output := make([]interface{}, 0)

	// TODO: flatten column mappings

	return output
}
