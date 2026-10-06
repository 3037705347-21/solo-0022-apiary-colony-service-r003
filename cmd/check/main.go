package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"example.com/apiary-colony-service/internal/app"
)

var workflows = map[string]func(*client) error{
	"register-apiary":    checkRegisterApiary,
	"install-hive":       checkHiveInstall,
	"record-inspection":  checkRecordInspection,
	"replace-queen":      checkReplaceQueen,
	"advance-hive-state": checkAdvanceHiveState,
	"read-apiary-status": checkReadApiaryStatus,
}

// workflowOrder 固定聚合校验的执行顺序，使重复运行得到稳定可比较的汇总。
var workflowOrder = []string{
	"register-apiary",
	"install-hive",
	"record-inspection",
	"replace-queen",
	"advance-hive-state",
	"read-apiary-status",
}

func main() {
	workflow := flag.String("workflow", "", "要校验的 workflow 编号")
	all := flag.Bool("all", false, "顺序运行全部 workflow 并输出聚合汇总")
	flag.Parse()

	if *all {
		os.Exit(runAllChecks())
	}

	scenario, ok := workflows[*workflow]
	if !ok {
		fmt.Fprintf(os.Stderr, "未知 workflow：%q\n", *workflow)
		os.Exit(2)
	}

	os.Exit(runSingleCheck(*workflow, scenario))
}

func runSingleCheck(id string, scenario func(*client) error) int {
	base, stop, err := startServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "启动服务失败：%v\n", err)
		return 1
	}
	defer stop()

	if err := scenario(newClient(base)); err != nil {
		fmt.Fprintf(os.Stderr, "workflow %s 校验失败：%v\n", id, err)
		return 1
	}
	fmt.Printf("workflow %s ok\n", id)
	return 0
}

// checkResult 记录单个 workflow 在聚合校验中的结论与失败阶段。
type checkResult struct {
	workflow string
	passed   bool
	stage    string
	err      error
}

const (
	stageStart = "start"
	stageCheck = "check"
)

func stageLabel(stage string) string {
	switch stage {
	case stageStart:
		return "启动服务"
	default:
		return "HTTP 校验"
	}
}

// runAllChecks 按固定顺序逐个运行全部 workflow：每个 workflow 都使用全新启动的
// 服务实例，并在结束后立即释放；任一 workflow 失败不影响其余 workflow 继续执行。
func runAllChecks() int {
	fmt.Printf("聚合校验开始：顺序运行全部 workflow（共 %d 个）\n", len(workflowOrder))

	results := make([]checkResult, 0, len(workflowOrder))
	failures := 0
	for index, id := range workflowOrder {
		result := checkResult{workflow: id}
		base, stop, startErr := startServer()
		if startErr != nil {
			result.stage = stageStart
			result.err = startErr
		} else {
			if checkErr := workflows[id](newClient(base)); checkErr != nil {
				result.stage = stageCheck
				result.err = checkErr
			}
			// 每个 workflow 结束后立即关闭并释放服务进程占用的监听资源。
			stop()
		}

		result.passed = result.err == nil
		if result.passed {
			fmt.Printf("[%d/%d] workflow %-20s 通过\n", index+1, len(workflowOrder), id)
		} else {
			failures++
			fmt.Printf("[%d/%d] workflow %-20s 失败（阶段：%s）：%v\n",
				index+1, len(workflowOrder), id, stageLabel(result.stage), result.err)
		}
		results = append(results, result)
	}

	fmt.Println("聚合校验汇总：")
	for _, result := range results {
		if result.passed {
			fmt.Printf("- %s: 通过\n", result.workflow)
			continue
		}
		fmt.Printf("- %s: 失败（阶段：%s）\n", result.workflow, stageLabel(result.stage))
	}
	passed := len(results) - failures
	if failures == 0 {
		fmt.Printf("整体结论：全部 %d 个 workflow 通过\n", len(results))
		return 0
	}
	fmt.Printf("整体结论：%d/%d 个 workflow 通过，%d 个失败\n", passed, len(results), failures)
	return 1
}

func startServer() (string, func(), error) {
	application := app.New(app.Config{Addr: "127.0.0.1:0"})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	server := &http.Server{Handler: application.Handler(), ReadHeaderTimeout: 5 * time.Second}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(listener)
	}()
	var stopped bool
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = listener.Close()
		<-serveDone
	}
	return "http://" + listener.Addr().String(), stop, nil
}
