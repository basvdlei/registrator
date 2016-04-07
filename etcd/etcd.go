package etcd

import (
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

func init() {
	bridge.Register(new(Factory), "etcd")
}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	urls := make([]string, 0)
	if uri.Host != "" {
		urls = append(urls, "http://"+uri.Host)
	} else {
		urls = append(urls, "http://127.0.0.1:2379")
	}

	res, err := http.Get(urls[0] + "/version")
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
		Endpoints: urls,
		// TODO Transport: etcd2.CancelableTransport{}
	}
	c, err := etcd2.New(cfg)

	return &EtcdAdapter{client2: c, path: uri.Path}
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
