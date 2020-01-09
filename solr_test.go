package dseutil

import (
	"testing"

	"github.com/neodarkstar/k8sutil"
)

const namespace = "uat01"
const fileName = "application.conf"
const metaGroupID = "santander50k_115rc1_2-MD"
const metaTopic = "ac.update.metadata.santander50k_115rc1_2"
const product = "acx-common"

var solrUtil SolrUtil
var util k8sutil.ACXK8sUtil

func TestGetSolrConfig(t *testing.T) {
	solrUtil = SolrUtil{}

	util =
		k8sutil.ACXK8sUtil{
			Clientset: k8sutil.BuildClientSet(),
			Namespace: namespace,
		}

	product := "acx-common"
	configMap := util.GetConfigMap(product, DefaultSolrFileName)
	config := solrUtil.GetSolrConfig(configMap)

	if config.Config.Ribbon.ListOfServers[0] == "" {
		t.Error("Solr Hosts Not Found")
	}
}

func TestValidateSolrConnectivity(t *testing.T) {
	configMap := util.GetConfigMap(product, DefaultSolrFileName)
	config := solrUtil.GetSolrConfig(configMap)

	listOfServers := config.Config.Ribbon.ListOfServers

	result, connections := solrUtil.ValidateSolrConnectivity(listOfServers)

	if result != true {
		t.Error("Connection is Closed")
	}

	if len(connections) != 3 {
		t.Error("Invalid Connection Count")
	}
}

func TestValidateSolrConnectivityInvalid(t *testing.T) {
	configMap := util.GetConfigMap(product, DefaultSolrFileName)
	config := solrUtil.GetSolrConfig(configMap)

	listOfServers := config.Config.Ribbon.ListOfServers

	listOfServers = append(listOfServers, "172.22.4.11:6666")

	result, connections := util.ValidateSolrConnectivity(listOfServers)

	if result != false {
		t.Error("Connection is Closed")
	}

	if len(connections) != 4 {
		t.Error("Invalid Connection Count")
	}
}
