package dseutil

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/neodarkstar/k8sutil"
)

const host = "172.22.4.21"
const username = "cassandra"
const password = "cassandra"

func TestValidateDSEConnectivity(t *testing.T) {
	util := DSEUtil{buildClusterConfig()}

	result, connections := util.ValidateDSEConnectivity()

	if result == false {
		failedConnections := make([]k8sutil.Connection, 0)

		for _, conn := range connections {
			if conn.Open == false {
				failedConnections = append(failedConnections, conn)
			}
		}
		t.Error(failedConnections)
	}
}

// func TestCreateKeyspace(t *testing.T) {
// 	util := DSEUtil{buildClusterConfig()}

// 	_, err := util.CreateKeyspace("unit_tests", 3, "SimpleStrategy", "replication_factor")

// 	if err != nil {
// 		t.Error("Got an Error")
// 	}
// }

// func TestDropKeyspace(t *testing.T) {
// 	util := DSEUtil{buildClusterConfig()}

// 	_, err := util.DropKeyspace("unit_tests")

// 	if err != nil {
// 		t.Error("Got an Error", err)
// 	}
// }

func buildClusterConfig() *gocql.ClusterConfig {
	cluster := gocql.NewCluster(host)

	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	cluster.CQLVersion = "3.0.0"
	cluster.NumConns = 1
	cluster.Timeout = time.Duration(10000 * time.Millisecond)

	return cluster
}
