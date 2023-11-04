package main

import (
	"flag"
	"fmt"

	"github.com/Sherlock-Zhu/MyFirstCSI/pkg/driver"
)

func main() {

	var (
		endpoint = flag.String("endpoint", "unix:///var/lib/csi/sockets/csi.sock", "Endpoint of gRPC server")
		nodeid   = flag.String("nodeid", "defaultValue", "node the disk to attach")
		region   = flag.String("region", "centralindia", "region to create the disk")
	)
	flag.Parse()

	fmt.Println(*endpoint, *nodeid, *region)

	// create a driver instance
	drv := driver.NewDriver(driver.InputParams{
		Name: driver.DefaultName,
		// unix:///var/lib/csi/sockets/csi.sock
		Endpoint: *endpoint,
		Region:   *region,
		Nodeid:   *nodeid,
	})

	// run on that driver instance, start gRPC server
	if err := drv.Run(); err != nil {
		fmt.Printf("Error %s, running the driver", err.Error())
	}
}
