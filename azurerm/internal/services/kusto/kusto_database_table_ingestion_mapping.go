package kusto

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	types "github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
	formats "github.com/Azure/azure-sdk-for-go/services/kusto/mgmt/2020-02-15/kusto"
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

type MappingCol struct {
	Column     string
	DataType   string
	Properties MappingColProperties
}

type MappingColProperties struct {
	Ordinal    string
	ConstValue string
	Path       string
	Field      string
	Transform  string
}

type CsvMapping struct {
	Name       string
	DataType   string
	Ordinal    string
	ConstValue string
}

type JsonMapping struct {
	column    string
	path      string
	datatype  string
	transform string
}

type AvroMapping struct {
	column    string
	field     string
	datatype  string
	transform string
}

type OrcMapping struct {
	column    string
	path      string
	datatype  string
	transform string
}

type ParquetMapping struct {
	column    string
	path      string
	datatype  string
	transform string
}

func resourceArmKustoDatabaseTableIngestionMapping() *schema.Resource {
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
					string(formats.AVRO),
					string(formats.CSV),
					string(formats.JSON),
					string(formats.ORC),
					string(formats.PARQUET),
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
							ValidateFunc: validation.StringInSlice([]string{
								string(types.Bool),
								string(types.DateTime),
								string(types.Dynamic),
								string(types.GUID),
								string(types.Int),
								string(types.Long),
								string(types.Real),
								string(types.String),
								string(types.Timespan),
								string(types.Decimal),
							}, false),
						},
						"properties": {
							Type:     schema.TypeList,
							MinItems: 1,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"path": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.StringIsNotEmpty,
									},
									"field": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.StringIsNotEmpty,
									},
									"ordinal": {
										Type:         schema.TypeInt,
										Optional:     true,
										ValidateFunc: validation.IntAtLeast(0),
									},
									"constant_value": {
										Type:         schema.TypeString,
										Optional:     true,
										ValidateFunc: validation.StringIsNotEmpty,
									},
									"transform": {
										Type:     schema.TypeString,
										Optional: true,
										ValidateFunc: validation.Any(validation.StringInSlice([]string{
											"PropertyBagArrayToDictionary",
											"SourceLocation",
											"SourceLineNumber",
											"DateTimeFromUnixSeconds",
											"DateTimeFromUnixMilliseconds",
											"DateTimeFromUnixMicroseconds",
											"DateTimeFromUnixNanoseconds",
										}, false),
											validation.StringMatch(
												regexp.MustCompile(`GetPathElement\(^\d+$\)`),
												`Invalid transform provided`),
										),
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

	log.Printf("[INFO] preparing arguments for Azure Kusto Database Table Mapping creation.")

	name := d.Get("name").(string)
	resourceGroupName := d.Get("resource_group_name").(string)
	clusterName := d.Get("cluster_name").(string)
	databaseName := d.Get("database_name").(string)
	tableName := d.Get("table_name").(string)
	kind := d.Get("kind").(string)
	mapping := d.Get("mapping").([]interface{})

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
	resourceID := fmt.Sprintf("%s/Tables/%s/Mappings/%s_%s", *readWriteDB.ID, tableName, kind, name)

	if d.IsNewResource() {
		stmtRaw := fmt.Sprintf(".show table %s ingestion %s mappings | where Name == \"%s\"", tableName, strings.ToLower(kind), name)

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
	columnMappings := expandKustoTableMapping(mapping, kind)
	stmtRaw := fmt.Sprintf(".create-or-alter table %s ingestion %s mapping \"%s\" '%s'", tableName, strings.ToLower(kind), name, *columnMappings)

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

	stmtRaw := fmt.Sprintf(".show table %s ingestion %s mappings | where Name == \"%s\"", id.Table, strings.ToLower(id.Kind), id.Name)

	stmt := kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(stmtRaw)
	iter, err := client.Mgmt(ctx, id.Database, stmt)
	if err != nil {
		return fmt.Errorf("Error reading Kusto Database Table Ingestion Mapping %q (Cluster %q, Database %q, Table %q): %+v", id.Name, id.Cluster, id.Database, id.Table, err)
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
	d.Set("kind", strings.ToUpper(recs[0].Kind))
	d.Set("mapping", flattenKustoTableMapping(recs[0].Mapping, strings.ToUpper(recs[0].Kind)))

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

	stmtRaw := fmt.Sprintf(".drop table %s ingestion %s mapping %s", id.Table, strings.ToLower(id.Kind), id.Name)
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

func expandKustoTableMapping(input []interface{}, kind string) (string, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("At least one column mapping must me be provided")
	}

	var mappings []MappingCol

	for _, v := range input {
		mapping := v.(map[string]interface{})

		props := mapping["properties"].(map[string]interface{})

		o := &MappingCol{
			Column:   mapping["column"].(string),
			DataType: mapping["data_type"].(string),
			Properties: &MappingColProperties{
				Path:       props["path"].(string),
				ConstValue: props["const_value"].(string),
				Field:      props["field"].(string),
				Ordinal:    props["ordinal"].(int),
				Transform:  props["transform"].(string),
			},
		}

		mappings = append(mappings, o)
	}

	mappingsJSON, err := json.Marshal(mappings)
	if err != nil {
		return nil, err
	}

	return string(mappingsJSON), nil
}

func flattenKustoTableMapping(input string, kind string) ([]interface{}, error) {
	if len(input) == 0 {
		return []interface{}{}
	}

	output := make([]interface{}, 0)

	var mappings []interface{}
	err := json.Unmarshal([]byte(input), &mappings)
	if err != nil {
		return nil, err
	}

	for _, m := range mappings {
		o := make(map[string]interface{}, 0)

		switch kind {
		case formats.AVRO:
			avro := m.(AvroMapping)
			o["column"] = avro.column
			o["data_type"] = avro.datatype
			props := make(map[string]interface{}, 0)
			o["properties"] = props
			props["field"] = avro.field
			props["transform"] = avro.transform
		case formats.CSV:
			csv := m.(CsvMapping)
			o["column"] = csv.Name
			o["data_type"] = csv.DataType
			props := make(map[string]interface{}, 0)
			o["properties"] = props
			props["ordinal"] = csv.Ordinal
			props["const_value"] = csv.ConstValue
		case formats.JSON:
		case formats.ORC:
		case formats.PARQUET:
			json := m.(JsonMapping)
			o["column"] = json.column
			o["data_type"] = json.datatype
			props := make(map[string]interface{}, 0)
			o["properties"] = props
			props["path"] = json.path
			props["transform"] = json.transform
		}

		output = append(output, o)
	}

	return output, nil
}
