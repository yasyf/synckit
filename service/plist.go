package service

import (
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

const (
	plistHeader = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
`
	plistFooter = `</dict>
</plist>
`
)

// renderPlist renders agent's launchd plist for the given tool config and executable
// path. The common keys (Label, ProgramArguments with [program, command], the PATH
// override, the log paths) render in a fixed order; the agent's ExtraKeys render
// between them sorted by key, so the output is deterministic regardless of Go map
// iteration order. The program is exe unless the agent sets Binary, in which case
// that binary is resolved on PATH. The result is valid plist XML, not byte-identical
// to any prior template.
func renderPlist(cfg ToolConfig, exe string, agent AgentSpec) (string, error) {
	logPath, err := homeJoin(cfg.LogName(cfg.FullLabel(agent)))
	if err != nil {
		return "", err
	}
	program, err := agentProgram(exe, agent)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString(plistHeader)
	writeString(&b, "Label", cfg.FullLabel(agent))

	b.WriteString("\t<key>ProgramArguments</key>\n\t<array>\n")
	fmt.Fprintf(&b, "\t\t<string>%s</string>\n", escape(program))
	fmt.Fprintf(&b, "\t\t<string>%s</string>\n", escape(agent.Command))
	b.WriteString("\t</array>\n")

	b.WriteString("\t<key>EnvironmentVariables</key>\n\t<dict>\n")
	fmt.Fprintf(&b, "\t\t<key>PATH</key>\n\t\t<string>%s</string>\n", escape(cfg.DaemonPATH))
	b.WriteString("\t</dict>\n")

	for _, key := range sortedKeys(agent.ExtraKeys) {
		if err := writeValue(&b, key, agent.ExtraKeys[key]); err != nil {
			return "", err
		}
	}

	writeString(&b, "StandardOutPath", logPath)
	writeString(&b, "StandardErrorPath", logPath)
	b.WriteString(plistFooter)
	return b.String(), nil
}

// agentProgram returns the program path for agent's plist: the resolved own-exe
// when Binary is empty, otherwise agent.Binary resolved on PATH. LookPath does not
// follow the final symlink, so a Homebrew shim like /opt/homebrew/bin/cookiesync is
// preserved and survives a brew relink — the same rationale as exePath.
func agentProgram(exe string, agent AgentSpec) (string, error) {
	if agent.Binary == "" {
		return exe, nil
	}
	path, err := exec.LookPath(agent.Binary)
	if err != nil {
		return "", fmt.Errorf("resolve agent binary %q on PATH: %w", agent.Binary, err)
	}
	return path, nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func writeValue(b *strings.Builder, key string, value any) error {
	switch v := value.(type) {
	case bool:
		fmt.Fprintf(b, "\t<key>%s</key>\n\t<%t/>\n", escape(key), v)
	case int:
		fmt.Fprintf(b, "\t<key>%s</key>\n\t<integer>%d</integer>\n", escape(key), v)
	case string:
		writeString(b, key, v)
	default:
		return fmt.Errorf("plist key %q has unsupported value type %T (want bool, int, or string)", key, value)
	}
	return nil
}

func writeString(b *strings.Builder, key, value string) {
	fmt.Fprintf(b, "\t<key>%s</key>\n\t<string>%s</string>\n", escape(key), escape(value))
}

func escape(s string) string {
	return strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
	).Replace(s)
}
