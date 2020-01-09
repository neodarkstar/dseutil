package dseutil

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"

	"github.com/gocql/gocql"
	"github.com/neodarkstar/k8sutil"
)

// DefaultCassandraPort Constant Port
const DefaultCassandraPort = "9042"

// DSEUtil Wrapper on all DSE functions
type DSEUtil struct {
	cluster *gocql.ClusterConfig
}

// DropKeyspace Drops a keyspace
func (d *DSEUtil) DropKeyspace(keyspace string) (*gocql.KeyspaceMetadata, error) {
	fmt.Printf("Dropping Keyspace: %s\n", keyspace)
	session, _ := d.cluster.CreateSession()
	defer session.Close()

	qerr := session.Query("Drop KEYSPACE " + keyspace).Exec()

	if qerr != nil {
		return nil, qerr
	}

	results, err := session.KeyspaceMetadata(keyspace)

	if err != nil {
		if err.Error() != gocql.ErrNoKeyspace.Error() {
			return nil, nil
		}
	}

	return results, nil
}

// CreateKeyspace batch create keyspace
func (d *DSEUtil) CreateKeyspace(keyspace string, replicationFactor int, class string, datacenter string) (*gocql.KeyspaceMetadata, error) {
	session, _ := d.cluster.CreateSession()
	defer session.Close()

	file, _ := os.Open("./init.cql")
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		regexpKeyspace := regexp.MustCompile(`<KEYSPACE>`)
		regexpClass := regexp.MustCompile(`<STRATEGY>`)
		regexpDatacenter := regexp.MustCompile(`<DATACENTER>`)
		regexpReplFactor := regexp.MustCompile(`<REPLICATION_FACTOR>`)
		regexpSemicolon := regexp.MustCompile(`;`)

		state1 := regexpKeyspace.ReplaceAllLiteralString(line, keyspace)
		state2 := regexpClass.ReplaceAllLiteralString(state1, class)
		state3 := regexpReplFactor.ReplaceAllLiteralString(state2, strconv.Itoa(replicationFactor))
		state4 := regexpDatacenter.ReplaceAllLiteralString(state3, datacenter)
		state5 := regexpSemicolon.ReplaceAllLiteralString(state4, "")

		err := session.Query(state5).Iter().Close()

		fmt.Printf("Executing: %s\n", state5)

		if err != nil {
			fmt.Println(err)
		}
	}

	meta, err := session.KeyspaceMetadata(keyspace)

	return meta, err
}

// ValidateDSEConnectivity Retrieves the listOfServers HostMap and tests for open ports
func (d *DSEUtil) ValidateDSEConnectivity() (bool, []k8sutil.Connection) {
	listOfServers := d.cluster.Hosts

	addrs := make([]net.TCPAddr, 0)

	for _, server := range listOfServers {
		ip := net.ParseIP(server)
		var h, p string

		if ip != nil {
			h = server
			p = DefaultCassandraPort
		} else {
			h, p, _ = net.SplitHostPort(server)
		}

		port, _ := strconv.Atoi(p)
		addrs = append(addrs, net.TCPAddr{
			IP:   net.ParseIP(h),
			Port: port,
		})
	}

	connections := k8sutil.ValidateConnectivity(&addrs)

	for _, conn := range connections {
		if conn.Open == false {
			panic(conn.Addr.IP.String() + " is not Open")
		}
	}

	session, _ := d.cluster.CreateSession()
	defer session.Close()

	itr := session.Query(`SELECT peer,native_transport_port FROM system.peers`).Iter()

	for {
		var peer string
		var port string

		// New map each iteration
		row := map[string]interface{}{
			"peer":                  &peer,
			"native_transport_port": &port,
		}
		if !itr.MapScan(row) {
			break
		}

		p, _ := strconv.Atoi(port)

		addrs = append(addrs, net.TCPAddr{
			IP:   net.ParseIP(peer),
			Port: p,
		})
	}

	connections = k8sutil.ValidateConnectivity(&addrs)

	allOpen := true

	for _, conn := range connections {
		if conn.Open == false {
			allOpen = false
		}
	}

	return allOpen, connections
}
