package cmd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/digitalocean/vulcan/config/zookeeper"
	pconfig "github.com/prometheus/prometheus/config"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	// ErrNoScrapeConfig is when the provided config omits or has a zero list of scrape configs
	ErrNoScrapeConfig = errors.New("no scrape config found")
	// ErrMultipleScrapeConfig is when the provided config has more than one scrape configs
	ErrMultipleScrapeConfig = errors.New("multiple scrape configs found")
	// ErrNoArgs is when the command line expected an arg but none was provided
	ErrNoArgs = errors.New("no args")
	// ErrMultipleArgs is when the command line expected one arge but multiple were provided
	ErrMultipleArgs = errors.New("multiple arguments in single argument command")
)

// Job returns an instantiated job subcommand
func Job() *cobra.Command {
	job := &cobra.Command{
		Use:   "job",
		Short: "job (set/delete/list) allows the operator to configure vulcan jobs",
	}

	job.PersistentFlags().String("zk-servers", "", "comma-separated list of zookeeper servers")
	job.PersistentFlags().String("zk-root", "/vulcan", "zookeeper root namespace")

	set := &cobra.Command{
		Use:   "set",
		Short: "set reads a job yaml description from stdin and sets it in zookeeper",
		RunE: func(cmd *cobra.Command, args []string) error {
			log.SetLevel(log.DebugLevel)
			// bind pflags to viper so they are settable by env variables
			cmd.Flags().VisitAll(func(f *pflag.Flag) {
				viper.BindPFlag(f.Name, f)
			})

			b, err := ioutil.ReadAll(os.Stdin)
			if err != nil {
				return err
			}
			c, err := pconfig.Load(string(b))
			if err != nil {
				return err
			}
			if len(c.ScrapeConfigs) == 0 {
				return ErrNoScrapeConfig
			}
			if len(c.ScrapeConfigs) > 1 {
				return ErrMultipleScrapeConfig
			}
			sc := c.ScrapeConfigs[0]
			name := sc.JobName

			client, _, err := zk.Connect(strings.Split(viper.GetString("zk-servers"), ","), time.Second*2)
			if err != nil {
				return err
			}
			s, err := zookeeper.NewStore(&zookeeper.Config{
				Root:   viper.GetString("zk-root"),
				Client: client,
			})
			if err != nil {
				return err
			}

			err = s.Set(name, b)
			if err != nil {
				return err
			}
			return nil
		},
	}

	list := &cobra.Command{
		Use:   "list",
		Short: "list jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			// bind pflags to viper so they are settable by env variables
			cmd.Flags().VisitAll(func(f *pflag.Flag) {
				viper.BindPFlag(f.Name, f)
			})

			client, _, err := zk.Connect(strings.Split(viper.GetString("zk-servers"), ","), time.Second*2)
			if err != nil {
				return err
			}
			s, err := zookeeper.NewStore(&zookeeper.Config{
				Root:   viper.GetString("zk-root"),
				Client: client,
			})
			if err != nil {
				return err
			}

			names, err := s.List()
			if err != nil {
				return err
			}
			for _, name := range names {
				fmt.Println(name)
			}
			return nil
		},
	}

	delete := &cobra.Command{
		Use:   "delete",
		Short: "deletes a sigle job",
		RunE: func(cmd *cobra.Command, args []string) error {
			// bind pflags to viper so they are settable by env variables
			cmd.Flags().VisitAll(func(f *pflag.Flag) {
				viper.BindPFlag(f.Name, f)
			})

			if len(args) == 0 {
				return ErrNoArgs
			}
			if len(args) > 1 {
				return ErrMultipleArgs
			}

			client, _, err := zk.Connect(strings.Split(viper.GetString("zk-servers"), ","), time.Second*2)
			if err != nil {
				return err
			}
			s, err := zookeeper.NewStore(&zookeeper.Config{
				Root:   viper.GetString("zk-root"),
				Client: client,
			})
			if err != nil {
				return err
			}

			err = s.Delete(args[0])
			if err != nil {
				return err
			}
			return nil
		},
	}

	job.AddCommand(set)
	job.AddCommand(list)
	job.AddCommand(delete)

	return job
}
