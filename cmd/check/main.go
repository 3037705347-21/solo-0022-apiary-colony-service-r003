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

// workflowOrder 固定聚合校验的执行顺序，保证多次运行得到稳定可比较的汇总。
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
	all := flag.Bool("all", false, "顺序运行全部 workflow 并输出通过/失败汇总")
	flag.Parse()

	if *all {
		os.Exit(runAll())
	}

	scenario, ok := workflows[*workflow]
	if !ok {
		fmt.Fprintf(os.Stderr, "未知 workflow：%q\n", *workflow)
		os.Exit(2)
	}

	base, stop, err := startServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "启动服务失败：%v\n", err)
		os.Exit(1)
	}
	defer stop()

	if err := scenario(newClient(base)); err != nil {
		fmt.Fprintf(os.Stderr, "workflow %s 校验失败：%v\n", *workflow, err)
		os.Exit(1)
	}
	fmt.Printf("workflow %s ok\n", *workflow)
}

// runAll 顺序运行全部 workflow：每个 workflow 使用独立的全新服务实例，
// 校验结束后立即释放该实例；即使某个 workflow 失败也继续跑完其余项，
// 最后逐条给出 PASS/FAIL 与整体结论，任一失败时返回非零退出码。
func runAll() int {
	type result struct {
		name string
		err  error
	}
	results := make([]result, 0, len(workflowOrder))
	failed := 0
	for _, name := range workflowOrder {
		err := runWorkflow(name, workflows[name])
		results = append(results, result{name: name, err: err})
		if err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "workflow %s 校验失败：%v\n", name, err)
		} else {
			fmt.Printf("workflow %s ok\n", name)
		}
	}

	passed := len(results) - failed
	fmt.Println("校验汇总：")
	for _, r := range results {
		if r.err == nil {
			fmt.Printf("- %s: PASS\n", r.name)
		} else {
			fmt.Printf("- %s: FAIL（%v）\n", r.name, r.err)
		}
	}
	if failed > 0 {
		fmt.Printf("整体结论：FAIL（%d/%d 通过）\n", passed, len(results))
		return 1
	}
	fmt.Printf("整体结论：PASS（%d/%d 通过）\n", passed, len(results))
	return 0
}

// runWorkflow 在全新服务实例上执行单个 workflow，并在返回前彻底释放该实例。
func runWorkflow(name string, scenario func(*client) error) error {
	base, stop, err := startServer()
	if err != nil {
		return fmt.Errorf("启动服务失败：%w", err)
	}
	defer stop()
	return scenario(newClient(base))
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
	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		// 等待 Serve 退出，确保监听端口与 goroutine 在下一个 workflow 启动前已释放。
		<-serveDone
	}
	return "http://" + listener.Addr().String(), stop, nil
}
