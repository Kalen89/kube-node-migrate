package nodemigrate

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

func validateTargetLabels(values map[string]string) error {
	for key, value := range values {
		if key == MigrationLabel && value != "true" {
			return fmt.Errorf("迁移标签必须为 %s=true", MigrationLabel)
		}
		if len(validation.IsQualifiedName(key)) > 0 || len(validation.IsValidLabelValue(value)) > 0 {
			return fmt.Errorf("目标标签无效: %s", key)
		}
		if key == labelKey || strings.HasPrefix(key, "kubernetes.io/") || strings.HasPrefix(key, "k8s.io/") || strings.HasPrefix(key, "node-role.kubernetes.io/") || strings.Contains(strings.Split(key, "/")[0], ".kubernetes.io") {
			return fmt.Errorf("禁止通过迁移参数修改系统身份或角色标签: %s", key)
		}
	}
	return nil
}
