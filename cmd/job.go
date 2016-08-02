package cmd

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var Job = &cobra.Command{
	Use:   "job",
	Short: "job places the provided stdin config on the zookeeper server as a configured job",
	RunE: func(cmd *cobra.Command, args []string) error {
		log.SetLevel(log.DebugLevel)
		// bind pflags to viper so they are settable by env variables
		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			viper.BindPFlag(f.Name, f)
		})

		job := "some-job"
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		p := path.Join(viper.GetString("zk-root"), "scraper", viper.GetString("pool"), "jobs", job)
		log.WithFields(log.Fields{
			"path": p,
			"job":  job,
		}).Debug("setting job into zookeeper")
		conn, _, err := zk.Connect(strings.Split(viper.GetString("zk-servers"), ","), time.Second*2)
		if err != nil {
			return err
		}
		exists, stat, err := conn.Exists(p)
		if exists {
			_, err = conn.Set(p, b, stat.Version)
			if err != nil {
				return err
			}
			return nil
		}
		_, err = conn.Create(p, b, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
		// todo verify config
		// todo add -p flag like mkdir -p to create full path
		return nil
	},
}

func init() {
	Job.Flags().String("zk-servers", "", "comma-separated list of zookeeper servers")
	Job.Flags().String("zk-root", "/vulcan", "zookeeper path to store config")
	Job.Flags().String("pool", "default", "the scraper pool to write the job to")
}
