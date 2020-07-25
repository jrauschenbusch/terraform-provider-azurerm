package parse

import (
	"fmt"
	"strings"

	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
)

type KustoDatabaseTableMappingId struct {
	ResourceGroup string
	Cluster       string
	Database      string
	Table         string
	Name          string
	Kind          string
}

func KustoDatabaseTableMappingID(input string) (*KustoDatabaseTableMappingId, error) {
	id, err := azure.ParseAzureResourceID(input)
	if err != nil {
		return nil, fmt.Errorf("[ERROR] Unable to parse Kusto Database Table ID %q: %+v", input, err)
	}

	tableMapping := &KustoDatabaseTableMappingId{
		ResourceGroup: id.ResourceGroup,
	}

	if tableMapping.Cluster, err = id.PopSegment("Clusters"); err != nil {
		return nil, err
	}

	if tableMapping.Database, err = id.PopSegment("Databases"); err != nil {
		return nil, err
	}

	if tableMapping.Table, err = id.PopSegment("Tables"); err != nil {
		return nil, err
	}

	mapping, err := id.PopSegment("Mappings")
	if err != nil {
		return nil, err
	}
	components := strings.Split(mapping, "_")
	if len(components) != 2 {
		return nil, fmt.Errorf("Azure Kusto Database Table Mapping ID should have 2 segments, got %d: '%s'", len(components), mapping)
	}
	tableMapping.Kind = components[0]
	tableMapping.Name = components[1]

	if err := id.ValidateNoEmptySegments(input); err != nil {
		return nil, err
	}

	return tableMapping, nil
}
