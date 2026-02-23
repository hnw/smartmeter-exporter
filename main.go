// Package main is a Prometheus exporter for ECHONET Lite smart meters.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hnw/go-smartmeter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- 1. メトリクスの定義 ---
var (
	// 電力 (W)
	powerGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "smartmeter_power_watts",
		Help: "Instantaneous electric power consumption in Watts",
	})
	// 電流 (A) - R相とT相をラベルで分ける
	currentGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "smartmeter_current_amperes",
		Help: "Instantaneous electric current in Amperes",
	}, []string{"phase"}) // phase="r" or "t"

	// 成功時刻 (Unix Timestamp) - データの鮮度確認用
	lastSuccessGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "smartmeter_last_scrape_timestamp_seconds",
		Help: "Unix timestamp of the last successful scrape",
	})

	// 通信時間 (秒)
	scrapeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "smartmeter_scrape_duration_seconds",
		Help:    "Scrape duration in seconds",
		Buckets: prometheus.DefBuckets,
	})

	// エラー回数カウンター（種類別）
	scrapeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartmeter_scrape_errors_total",
		Help: "Total number of failed scrapes, labeled by error type",
	}, []string{"type"})
)

const (
	errorTypeIPResolve = "ip_resolve"
	errorTypeAuth      = "auth"
	errorTypeQuery     = "query"
	errorTypeParse     = "parse"
)

const (
	reAuthCooldown   = 5 * time.Second
	postAuthCooldown = 2 * time.Second
)

func init() {
	// メトリクスを登録
	prometheus.MustRegister(powerGauge)
	prometheus.MustRegister(currentGauge)
	prometheus.MustRegister(lastSuccessGauge)
	prometheus.MustRegister(scrapeDuration)
	prometheus.MustRegister(scrapeErrors)
}

func main() {
	// --- 2. 設定の読み込み ---
	var (
		bRouteID    = getEnv("SMARTMETER_ID", "")
		bRoutePass  = getEnv("SMARTMETER_PASSWORD", "")
		devicePath  = getEnv("SMARTMETER_DEVICE", "/dev/ttyACM0")
		intervalStr = getEnv("SMARTMETER_INTERVAL", "60")
		listenPort  = getEnv("SMARTMETER_PORT", "9102")
		channel     = getEnv("SMARTMETER_CHANNEL", "")
		ipAddr      = getEnv("SMARTMETER_IPADDR", "")
		useDSE      = false
		verbosity   = 1
	)

	if v := os.Getenv("SMARTMETER_DSE"); v != "false" && v != "0" {
		useDSE = true
	}
	if v := os.Getenv("SMARTMETER_VERBOSITY"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			verbosity = i
		}
	}

	flag.StringVar(&bRouteID, "id", bRouteID, "B-route ID")
	flag.StringVar(&bRoutePass, "password", bRoutePass, "B-route password")
	flag.StringVar(&devicePath, "device", devicePath, "Serial port device path")
	flag.StringVar(
		&intervalStr,
		"interval",
		intervalStr,
		"Scrape interval in seconds (default: 60)",
	)
	flag.StringVar(&listenPort, "port", listenPort, "Exporter listen port (default: 9102)")
	flag.StringVar(&channel, "channel", channel, "Fixed Wi-SUN Channel (skip scan)")
	flag.StringVar(&ipAddr, "ipaddr", ipAddr, "Fixed Smart Meter IPv6 Address (skip scan)")
	flag.BoolVar(&useDSE, "dse", useDSE, "Enable Dual Stack Edition (DSE)")
	flag.IntVar(&verbosity, "verbosity", verbosity, "Log verbosity (0:quiet, 3:debug)")

	flag.Parse()

	logger := newLogger(verbosity)
	slog.SetDefault(logger)
	smLogger := slog.NewLogLogger(logger.Handler(), slog.LevelInfo)

	if bRouteID == "" || bRoutePass == "" {
		logger.Error("ID and Password are required via flags or env vars")
		os.Exit(1)
	}

	intervalSec, err := strconv.Atoi(intervalStr)
	if err != nil || intervalSec < 10 {
		logger.Warn(
			"Invalid interval, using default",
			"interval",
			intervalStr,
			"default_seconds",
			60,
		)
		intervalSec = 60
	}

	// --- 3. デバイスの初期化 ---
	// smartmeter.Open に渡すオプションを動的に構築
	smOpts := []smartmeter.Option{
		smartmeter.ID(bRouteID),
		smartmeter.Password(bRoutePass),
		smartmeter.DualStackSK(useDSE),
		smartmeter.Verbosity(verbosity),
		smartmeter.Logger(smLogger),
		smartmeter.RetryInterval(5 * time.Second),
	}

	// Channel指定がある場合のみ追加
	if channel != "" {
		smOpts = append(smOpts, smartmeter.Channel(channel))
	}
	// IP指定がある場合のみ追加
	if ipAddr != "" {
		smOpts = append(smOpts, smartmeter.IPAddr(ipAddr))
	}

	dev, err := smartmeter.Open(devicePath, smOpts...)
	if err != nil {
		logger.Error("Failed to open device", "error", err, "device", devicePath)
		os.Exit(1)
	}

	// --- 4. バックグラウンド取得ループの開始 ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go runScrapeLoop(ctx, dev, time.Duration(intervalSec)*time.Second, logger)

	// --- 5. HTTPサーバー起動 ---
	http.Handle("/metrics", promhttp.Handler())

	logger.Info("Starting Prometheus exporter", "port", listenPort)
	logger.Info(
		"Device configured",
		"device",
		devicePath,
		"interval_seconds",
		intervalSec,
		"dse",
		useDSE,
	)

	server := &http.Server{
		Addr:              ":" + listenPort,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Graceful Shutdown用
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	<-stopChan
	logger.Info("Shutting down")
	cancel() // ループを停止
	ctxShut, cancelShut := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShut()
	if err := server.Shutdown(ctxShut); err != nil {
		logger.Warn("HTTP server shutdown error", "error", err)
	}
}

// runScrapeLoop は定期的にデータを取得します。
// スマートメーターの応答遅延（約30秒）によるタイムアウトを回避するため、
// バックグラウンドで非同期に取得し、HTTP要求には直近のキャッシュを返します。
func runScrapeLoop(
	ctx context.Context,
	dev *smartmeter.Device,
	interval time.Duration,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 起動時にまず1回実行
	logger.Info("First scrape starting")
	scrape(dev, logger)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scrape(dev, logger)
		}
	}
}

// 実際のデータ取得ロジック
func scrape(dev *smartmeter.Device, logger *slog.Logger) {
	start := time.Now()
	defer func(start time.Time) {
		scrapeDuration.Observe(time.Since(start).Seconds())
	}(start)

	// IPアドレス解決 (初回のみ、またはロスト時)
	if dev.IPAddr == "" {
		ipAddr, err := dev.GetNeibourIP()
		if err != nil {
			logger.Warn("Failed to scan neighbor IP", "error", err)
			scrapeErrors.WithLabelValues(errorTypeIPResolve).Inc()
			return
		}
		dev.IPAddr = ipAddr
	}

	// プロパティ要求 (電力と電流)
	request := smartmeter.NewFrame(
		smartmeter.LvSmartElectricEnergyMeter,
		smartmeter.Get,
		[]*smartmeter.Property{
			smartmeter.NewProperty(
				smartmeter.LvSmartElectricEnergyMeter_InstantaneousElectricPower,
				nil,
			),
			smartmeter.NewProperty(smartmeter.LvSmartElectricEnergyMeter_InstantaneousCurrent, nil),
		},
	)

	// クエリ実行
	response, err := dev.QueryEchonetLite(request, smartmeter.Retry(3))
	if err != nil {
		logger.Info("Query failed, attempting re-auth", "error", err)
		logger.Debug("Waiting before re-auth", "cooldown", reAuthCooldown.String())
		time.Sleep(reAuthCooldown)
		// 失敗時は再認証を試みる
		if authErr := dev.Authenticate(); authErr != nil {
			logger.Warn("Authentication failed", "error", authErr)
			scrapeErrors.WithLabelValues(errorTypeAuth).Inc()
			return
		}
		logger.Info("Re-authentication successful")
		logger.Debug("Waiting before retrying query", "cooldown", postAuthCooldown.String())
		time.Sleep(postAuthCooldown)
		// 再試行
		response, err = dev.QueryEchonetLite(request, smartmeter.Retry(3))
		if err != nil {
			logger.Warn("Query failed after re-auth", "error", err)
			scrapeErrors.WithLabelValues(errorTypeQuery).Inc()
			return
		}
	}

	// 値のパースとメトリクス更新
	parseAndSetMetrics(response, logger)
}

func parseAndSetMetrics(response *smartmeter.Frame, logger *slog.Logger) {
	foundData := false
	for _, p := range response.Properties {
		switch p.EPC {
		case smartmeter.LvSmartElectricEnergyMeter_InstantaneousElectricPower:
			val := float64(binary.BigEndian.Uint32(p.EDT))
			powerGauge.Set(val)
			foundData = true
		case smartmeter.LvSmartElectricEnergyMeter_InstantaneousCurrent:
			r := float64(binary.BigEndian.Uint16(p.EDT[:2])) / 10.0
			t := float64(binary.BigEndian.Uint16(p.EDT[2:])) / 10.0
			currentGauge.WithLabelValues("r").Set(r)
			currentGauge.WithLabelValues("t").Set(t)
			foundData = true
		}
	}

	if foundData {
		lastSuccessGauge.Set(float64(time.Now().Unix()))
		logger.Debug("Scrape successful")
	} else {
		logger.Warn("Response contained no recognized properties")
		scrapeErrors.WithLabelValues(errorTypeParse).Inc()
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func newLogger(verbosity int) *slog.Logger {
	level := levelFromVerbosity(verbosity)
	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: dropTimeAttr,
	}

	switch logFormat() {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	default:
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
}

func levelFromVerbosity(verbosity int) slog.Level {
	switch {
	case verbosity <= 0:
		return slog.LevelError
	case verbosity == 1:
		return slog.LevelInfo
	default:
		return slog.LevelDebug
	}
}

func logFormat() string {
	if v := strings.ToLower(os.Getenv("SMARTMETER_LOG_FORMAT")); v != "" {
		if v == "json" || v == "text" {
			return v
		}
	}
	return "text"
}

func dropTimeAttr(_ []string, attr slog.Attr) slog.Attr {
	if attr.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return attr
}
