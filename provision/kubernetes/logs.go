// Copyright 2020 tsuru authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kubernetes

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tsuru/tsuru/provision"
	appTypes "github.com/tsuru/tsuru/types/app"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	knet "k8s.io/apimachinery/pkg/util/net"
)

const (
	logLineTimeSeparator = ' '
	logLineSeparator     = '\n'
	logWatchBufferSize   = 1000
)

var (
	watchNewPodsInterval = time.Second * 10
	watchTimeout         = time.Hour
)

func (p *kubernetesProvisioner) ListLogs(app appTypes.App, args appTypes.ListLogArgs) ([]appTypes.Applog, error) {
	clusterClient, err := clusterForPool(app.GetPool())
	if err != nil {
		return nil, err
	}
	if !clusterClient.LogsFromAPIServerEnabled() {
		return nil, provision.ErrLogsUnavailable
	}
	clusterController, err := getClusterController(GetProvisioner(), clusterClient)
	if err != nil {
		return nil, err
	}

	ns, err := clusterClient.AppNamespace(app)
	if err != nil {
		return nil, err
	}

	podInformer, err := clusterController.getPodInformer()
	if err != nil {
		return nil, err
	}

	pods, err := podInformer.Lister().Pods(ns).List(listPodsSelectorForLog(args))
	return listLogsFromPods(clusterClient, ns, pods, args)
}

func (p *kubernetesProvisioner) WatchLogs(app appTypes.App, args appTypes.ListLogArgs) (appTypes.LogWatcher, error) {
	clusterClient, err := clusterForPool(app.GetPool())
	if err != nil {
		return nil, err
	}
	if !clusterClient.LogsFromAPIServerEnabled() {
		return nil, provision.ErrLogsUnavailable
	}
	clusterClient.SetTimeout(watchTimeout)

	clusterController, err := getClusterController(GetProvisioner(), clusterClient)
	if err != nil {
		return nil, err
	}

	ns, err := clusterClient.AppNamespace(app)
	if err != nil {
		return nil, err
	}

	podInformer, err := clusterController.getPodInformer()
	if err != nil {
		return nil, err
	}

	selector := listPodsSelectorForLog(args)
	pods, err := podInformer.Lister().Pods(ns).List(selector)
	if err != nil {
		return nil, err
	}
	watchingPods := map[string]bool{}
	ctx, done := context.WithCancel(context.Background())
	watcher := &k8sLogsNotifier{
		ctx:           ctx,
		ch:            make(chan appTypes.Applog, logWatchBufferSize),
		ns:            ns,
		clusterClient: clusterClient,
		done:          done,
	}
	for _, pod := range pods {
		if pod.Status.Phase == apiv1.PodPending {
			continue
		}
		watchingPods[pod.ObjectMeta.Name] = true
		go watcher.watchPod(pod)
	}

	// PodListers does not support to watch events
	// this is a tricky way to watch new pods
	// it does not increase kubernetes apiserver usage
	// cause listers use local memory to do list operations
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(watchNewPodsInterval):
				pods, err := podInformer.Lister().Pods(ns).List(selector)
				if err != nil {
					watcher.ch <- errToLog("tsuru", args.AppName, err)
					continue
				}

				for _, pod := range pods {
					_, alreadyWatching := watchingPods[pod.ObjectMeta.Name]
					if !alreadyWatching && pod.Status.Phase != apiv1.PodPending {
						watcher.ch <- infoToLog(args.AppName, "Starting to watch unit: "+pod.ObjectMeta.Name)
						watchingPods[pod.ObjectMeta.Name] = true
						go watcher.watchPod(pod)
					}
				}
			}
		}
	}()

	return watcher, nil
}

func listLogsFromPods(clusterClient *ClusterClient, ns string, pods []*apiv1.Pod, args appTypes.ListLogArgs) ([]appTypes.Applog, error) {
	var wg sync.WaitGroup
	wg.Add(len(pods))

	errs := make([]error, len(pods))
	logs := make([][]appTypes.Applog, len(pods))
	tailLimit := tailLines(args.Limit)
	if args.Limit == 0 {
		tailLimit = tailLines(100)
	}

	for index, pod := range pods {
		go func(index int, pod *apiv1.Pod) {
			defer wg.Done()

			request := clusterClient.CoreV1().Pods(ns).GetLogs(pod.ObjectMeta.Name, &apiv1.PodLogOptions{
				TailLines:  tailLimit,
				Timestamps: true,
			})
			data, err := request.DoRaw()
			if err != nil {
				errs[index] = err
				return
			}

			appName := pod.ObjectMeta.Labels["tsuru.io/app-name"]
			appProcess := pod.ObjectMeta.Labels["tsuru.io/app-process"]
			lines := bytes.Split(data, []byte{logLineSeparator})
			tsuruLogs := make([]appTypes.Applog, 0, len(lines))
			for _, line := range lines {
				if len(line) == 0 {
					continue
				}
				tsuruLog := parsek8sLogLine(line)
				tsuruLog.Unit = pod.ObjectMeta.Name
				tsuruLog.AppName = appName
				tsuruLog.Source = appProcess
				tsuruLogs = append(tsuruLogs, tsuruLog)
			}
			logs[index] = tsuruLogs
		}(index, pod)
	}

	wg.Wait()

	unifiedLog := []appTypes.Applog{}
	for _, podLogs := range logs {
		unifiedLog = append(unifiedLog, podLogs...)
	}

	sort.Slice(unifiedLog, func(i, j int) bool { return unifiedLog[i].Date.Before(unifiedLog[j].Date) })

	for index, err := range errs {
		if err == nil {
			continue
		}

		pod := pods[index]
		appName := pod.ObjectMeta.Labels["tsuru.io/app-name"]

		unifiedLog = append(unifiedLog, errToLog(pod.ObjectMeta.Name, appName, err))
	}

	return unifiedLog, nil
}

func listPodsSelectorForLog(args appTypes.ListLogArgs) labels.Selector {
	return labels.SelectorFromSet(labels.Set(map[string]string{
		"tsuru.io/app-name": args.AppName,
	}))
}

func parsek8sLogLine(line []byte) (appLog appTypes.Applog) {
	parts := bytes.SplitN(line, []byte{logLineTimeSeparator}, 2)

	if len(parts) < 2 {
		appLog.Message = string(line)
		return
	}
	appLog.Date, _ = parseRFC3339(string(parts[0]))
	appLog.Message = string(parts[1])

	return
}

func parseRFC3339(s string) (time.Time, error) {
	if t, timeErr := time.Parse(time.RFC3339Nano, s); timeErr == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

func tailLines(i int) *int64 {
	b := int64(i)
	return &b
}

func errToLog(podName, appName string, err error) appTypes.Applog {
	return appTypes.Applog{
		Date:    time.Now().UTC(),
		Message: fmt.Sprintf("Could not get logs from unit: %s, error: %s", podName, err.Error()),
		Unit:    "apiserver",
		AppName: appName,
		Source:  "kubernetes",
	}
}

func infoToLog(appName string, message string) appTypes.Applog {
	return appTypes.Applog{
		Date:    time.Now().UTC(),
		Message: message,
		Unit:    "apiserver",
		AppName: appName,
		Source:  "kubernetes",
	}
}

type k8sLogsNotifier struct {
	ctx  context.Context
	ch   chan appTypes.Applog
	ns   string
	wg   sync.WaitGroup
	once sync.Once
	done context.CancelFunc

	clusterClient *ClusterClient
}

func (k *k8sLogsNotifier) watchPod(pod *apiv1.Pod) {
	k.wg.Add(1)
	defer k.wg.Done()

	appName := pod.ObjectMeta.Labels["tsuru.io/app-name"]
	appProcess := pod.ObjectMeta.Labels["tsuru.io/app-process"]

	var tailLines int64
	request := k.clusterClient.CoreV1().Pods(k.ns).GetLogs(pod.ObjectMeta.Name, &apiv1.PodLogOptions{
		Follow:     true,
		TailLines:  &tailLines,
		Timestamps: true,
	})
	stream, err := request.Stream()
	if err != nil {
		k.ch <- errToLog(pod.ObjectMeta.Name, appName, err)
		return
	}

	go func() {
		// TODO: after update k8s library we can put context inside the request
		<-k.ctx.Done()
		stream.Close()
	}()

	reader := bufio.NewReader(stream)

	for {
		line, err := reader.ReadBytes('\n')

		if knet.IsProbableEOF(err) {
			break
		}

		if err != nil {
			k.ch <- errToLog(pod.ObjectMeta.Name, appName, err)
			break
		}

		line = bytes.Trim(line, "\n")

		tsuruLog := parsek8sLogLine(line)
		tsuruLog.Unit = pod.ObjectMeta.Name
		tsuruLog.AppName = appName
		tsuruLog.Source = appProcess

		k.ch <- tsuruLog
	}

}

func (k *k8sLogsNotifier) Chan() <-chan appTypes.Applog {
	return k.ch
}

func (k *k8sLogsNotifier) Close() {
	k.once.Do(func() {
		k.done()
		k.wg.Wait()
		close(k.ch)
	})
}
