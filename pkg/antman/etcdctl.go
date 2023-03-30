package antman

import (
	"os"
	"os/exec"
	"strings"

	"k8s.io/klog"
)

type EtcdWrapper struct {
	basic_etcd_cmd string
	domain         string
}

func NewEtcdWrapper() *EtcdWrapper {
	api_server_ip := "10.10.108.91"
	api_server_port := "2379"
	pem_prefix := "/etc/kubernetes/pki/etcd"

	var cmd string
	cmd = "etcd --endpoints=https://" + api_server_ip + ":" + api_server_port + " "
	cmd += "--cacert=" + pem_prefix + "/ca.crt "
	cmd += "--cert=" + pem_prefix + "/peer.crt "
	cmd += "--key=" + pem_prefix + "/ca.key "

	os.Setenv("ETCDCTL_APP", "3")

	ew := &EtcdWrapper{
		basic_etcd_cmd: cmd,
		domain:         "/antman",
	}
	return ew
}

func (ew *EtcdWrapper) ReadEtcd(key *string) *string {
	cmd := ew.basic_etcd_cmd
	cmd += "get "
	cmd += ew.domain + "/" + *key + " "

	cmds := strings.Split(cmd, " ")
	out, err := exec.Command(cmds[0], cmds[1:]...).CombinedOutput()
	if err != nil {
		klog.Error("cmd.Run() failed with %v\n", err)
		return nil
	}

	outstr := string(out)

	tokens := strings.Split(outstr, "\n")

	if len(tokens) < 2 {
		return nil
	}

	return &tokens[1]
}

func (ew *EtcdWrapper) WriteEtcd(key *string, val *string) {
	cmd := ew.basic_etcd_cmd
	cmd += "put "
	cmd += ew.domain + "/" + *key + " " + *val

	cmds := strings.Split(cmd, " ")
	_, err := exec.Command(cmds[0], cmds[1:]...).CombinedOutput()
	if err != nil {
		klog.Errorf("cmd.Run() failed with %v\n", err)
	}

	return
}
