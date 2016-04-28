package etcd

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"golang.org/x/net/context"

	etcd2 "github.com/coreos/etcd/client"
	"github.com/gliderlabs/registrator/bridge"
	etcd "gopkg.in/coreos/go-etcd.v0/etcd"
)

var certFile = flag.String("etcd-cert-file", "", "identify HTTPS client using this SSL certificate file")
var keyFile = flag.String("etcd-key-file", "", "identify HTTPS client using this SSL key file")
var caFile = flag.String("etcd-ca-file", "", "verify certificates of HTTPS-enabled servers using this CA bundle")

func init() {
	bridge.Register(new(Factory), "etcd")
}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	transport, err := createTransport()
	if err != nil {
		log.Fatal("etcd: error creating transport", err)
	}

	urls := make([]string, 0)

	scheme := "http://"
	if *caFile != "" || *certFile != "" {
		scheme = "https://"
	}

	if uri.Host != "" {
		urls = append(urls, scheme+uri.Host)
	} else {
		urls = append(urls, scheme+"127.0.0.1:2379")
	}

	c := http.Client{
		Transport: transport,
	}
	res, err := c.Get(urls[0] + "/version")
	if err != nil {
		log.Fatal("etcd: error retrieving version", err)
	}

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	if match, _ := regexp.Match("0\\.4\\.*", body); match == true {
		log.Println("etcd: using v0 client")
		return &EtcdAdapter{client: etcd.NewClient(urls), path: uri.Path}
	}

	cfg := etcd2.Config{
		Endpoints:               urls,
		HeaderTimeoutPerRequest: time.Duration(3) * time.Second,
		Transport:               transport,
	}

	client2, err := etcd2.New(cfg)
	if err != nil {
		log.Fatal("etcd: no valid etcd client could be created", err)
	}

	return &EtcdAdapter{client2: client2, path: uri.Path}
}

type EtcdAdapter struct {
	client  *etcd.Client
	client2 etcd2.Client

	path string
}

func (r *EtcdAdapter) Ping() error {
	r.syncEtcdCluster()

	var err error
	if r.client != nil {
		rr := etcd.NewRawRequest("GET", "version", nil, nil)
		_, err = r.client.SendRequest(rr)
	} else {
		kapi := etcd2.NewKeysAPI(r.client2)
		_, err = kapi.Get(context.Background(), "/", &etcd2.GetOptions{})
	}

	if err != nil {
		return err
	}
	return nil
}

func (r *EtcdAdapter) syncEtcdCluster() {
	var result bool
	if r.client != nil {
		result = r.client.SyncCluster()
	} else {
		err := r.client2.Sync(context.Background())
		if err == nil {
			result = true
		}
	}

	if !result {
		log.Println("etcd: sync cluster was unsuccessful")
	}
}

func (r *EtcdAdapter) Register(service *bridge.Service) error {
	r.syncEtcdCluster()

	path := r.path + "/" + service.Name + "/" + service.ID
	port := strconv.Itoa(service.Port)
	addr := net.JoinHostPort(service.IP, port)

	var err error
	if r.client != nil {
		_, err = r.client.Set(path, addr, uint64(service.TTL))
	} else {
		kapi := etcd2.NewKeysAPI(r.client2)
		_, err = kapi.Set(context.Background(), path, addr, &etcd2.SetOptions{TTL: time.Duration(service.TTL) * time.Second})
	}

	if err != nil {
		log.Println("etcd: failed to register service:", err)
	}
	return err
}

func (r *EtcdAdapter) Deregister(service *bridge.Service) error {
	r.syncEtcdCluster()

	path := r.path + "/" + service.Name + "/" + service.ID

	var err error
	if r.client != nil {
		_, err = r.client.Delete(path, false)
	} else {
		kapi := etcd2.NewKeysAPI(r.client2)
		_, err = kapi.Delete(context.Background(), path, &etcd2.DeleteOptions{})
	}

	if err != nil {
		log.Println("etcd: failed to deregister service:", err)
	}
	return err
}

func (r *EtcdAdapter) Refresh(service *bridge.Service) error {
	return r.Register(service)
}

func (r *EtcdAdapter) Services() ([]*bridge.Service, error) {
	return []*bridge.Service{}, nil
}

func createTransport() (*http.Transport, error) {
	var transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: false,
	}

	if *caFile != "" {
		certBytes, err := ioutil.ReadFile(*caFile)
		if err != nil {
			return &http.Transport{}, err
		}

		caCertPool := x509.NewCertPool()
		ok := caCertPool.AppendCertsFromPEM(certBytes)

		if ok {
			tlsConfig.RootCAs = caCertPool
		}
	}

	if *certFile != "" && *keyFile != "" {
		tlsCert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			return &http.Transport{}, err
		}
		tlsConfig.Certificates = []tls.Certificate{tlsCert}
	}

	transport.TLSClientConfig = tlsConfig
	return transport, nil
}
