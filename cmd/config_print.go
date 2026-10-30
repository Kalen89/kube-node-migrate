package cmd

import (
	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

func newConfigCommand() *cobra.Command {
	config := &cobra.Command{Use: "config", Short: "Manage migration configuration", Args: cobra.NoArgs}
	print := &cobra.Command{Use: "print", Short: "Print configuration", Args: cobra.NoArgs}
	print.AddCommand(&cobra.Command{
		Use: "defaults", Short: "Print static default configuration as YAML", Args: cobra.NoArgs,
		Example: "  kube-node-migrate config print defaults > migration.yaml",
		RunE: func(cmd *cobra.Command, _ []string) error {
			data, err := yaml.Marshal(defaultPlanConfig())
			if err != nil {
				return err
			}
			_, err = cmd.OutOrStdout().Write(data)
			return err
		},
	})
	config.AddCommand(print)
	return config
}
