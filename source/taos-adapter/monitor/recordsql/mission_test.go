package recordsql

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
)

func TestGlobalRecordMission(t *testing.T) {
	t.Run("Test get on nil", func(t *testing.T) {
		got := getMission(RecordTypeSQL)
		if got != nil {
			t.Errorf("Expected nil, got %v", got)
		}
	})

	t.Run("Test set and get", func(t *testing.T) {
		testRecord := &RecordMission{}

		setMission(RecordTypeSQL, testRecord)

		got := getMission(RecordTypeSQL)
		if got != testRecord {
			t.Errorf("Expected %v, got %v", testRecord, got)
		}
		setMission(RecordTypeSQL, nil)
	})

	t.Run("Test concurrent access", func(t *testing.T) {

		var wg sync.WaitGroup
		iterations := 1000
		testRecord := &RecordMission{}

		wg.Add(iterations * 2)
		for i := 0; i < iterations; i++ {
			go func() {
				defer wg.Done()
				setMission(RecordTypeSQL, testRecord)
			}()
			go func() {
				defer wg.Done()
				_ = getMission(RecordTypeSQL)
			}()
		}
		wg.Wait()

		got := getMission(RecordTypeSQL)
		if got != testRecord {
			t.Errorf("Expected %v after concurrent access, got %v", testRecord, got)
		}
		setMission(RecordTypeSQL, nil)
	})
}

func TestStartRecordSql(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	validStart := time.Now().Add(1 * time.Hour).Format(InputTimeFormat)
	validEnd := time.Now().Add(2 * time.Hour).Format(InputTimeFormat)
	validOutFile := "output.csv"

	// Mock global state
	originalGlobal := getMission(RecordTypeSQL)
	defer setMission(RecordTypeSQL, originalGlobal)

	tests := []struct {
		name        string
		startTime   string
		endTime     string
		logPath     string
		outFile     string
		location    string
		preTest     func()
		postTest    func()
		expectedErr string
	}{
		{
			name:        "already running",
			startTime:   validStart,
			endTime:     validEnd,
			logPath:     tempDir,
			outFile:     validOutFile,
			preTest:     func() { setMission(RecordTypeSQL, &RecordMission{recordType: RecordTypeSQL, running: true}) },
			postTest:    func() { setMission(RecordTypeSQL, nil) },
			expectedErr: "record mission is already running",
		},
		{
			name:        "invalid location format",
			startTime:   validStart,
			endTime:     validEnd,
			logPath:     tempDir,
			outFile:     validOutFile,
			location:    "Invalid/Location",
			expectedErr: "invalid location format",
		},
		{
			name:        "invalid start time format",
			startTime:   "invalid-time",
			endTime:     validEnd,
			logPath:     tempDir,
			outFile:     validOutFile,
			expectedErr: "invalid start time format",
		},
		{
			name:        "invalid end time format",
			startTime:   validStart,
			endTime:     "invalid-time",
			logPath:     tempDir,
			outFile:     validOutFile,
			expectedErr: "invalid end time format",
		},
		{
			name:        "start time after end time",
			startTime:   time.Now().Add(2 * time.Hour).Format(InputTimeFormat),
			endTime:     time.Now().Add(1 * time.Hour).Format(InputTimeFormat),
			logPath:     tempDir,
			outFile:     validOutFile,
			expectedErr: "is after end time",
		},
		{
			name:        "end time in past",
			startTime:   time.Now().Add(-2 * time.Hour).Format(InputTimeFormat),
			endTime:     time.Now().Add(-1 * time.Hour).Format(InputTimeFormat),
			logPath:     tempDir,
			outFile:     validOutFile,
			expectedErr: "is in the past",
		},
		{
			name:        "successful start",
			startTime:   validStart,
			endTime:     validEnd,
			logPath:     tempDir,
			outFile:     validOutFile,
			postTest:    func() { StopRecordSqlMission() },
			expectedErr: "",
		},
		{
			name:      "successful with location",
			startTime: validStart,
			endTime:   validEnd,
			logPath:   tempDir,
			outFile:   "with_location.csv",
			location:  "UTC",
			postTest:  func() { StopRecordSqlMission() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset global state before each test
			setMission(RecordTypeSQL, nil)

			if tt.preTest != nil {
				tt.preTest()
			}

			// For the "output file not in log path" case, we need to create the parent dir
			if tt.name == "output file not in log path" {
				parentDir := filepath.Join(tempDir, "..")
				err := os.MkdirAll(parentDir, 0755)
				require.NoError(t, err)
			}
			out := filepath.Join(tt.logPath, tt.outFile)
			f, err := os.Create(out)
			require.NoError(t, err)
			defer func() {
				err = f.Close()
				assert.NoError(t, err)
				err = os.Remove(out)
				assert.NoError(t, err)
			}()
			err = StartRecordWithTestWriter(RecordTypeSQL, tt.startTime, tt.endTime, tt.location, f)

			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
				mission := getMission(RecordTypeSQL)
				require.NotNil(t, mission)
				assert.False(t, mission.running) // should be false until start time
				assert.NotNil(t, mission.csvWriter)
				assert.NotNil(t, mission.ctx)
				assert.NotNil(t, mission.cancelFunc)
			}

			if tt.postTest != nil {
				tt.postTest()
			}
		})
	}
}

func TestStartRecordSqlWithRotateFail(t *testing.T) {
	oldPath := config.Conf.Log.Path
	defer func() {
		config.Conf.Log.Path = oldPath
	}()
	if globalSQLRotateWriter != nil {
		err := globalSQLRotateWriter.Close()
		assert.NoError(t, err, "Failed to close existing globalSQLRotateWriter")
		globalSQLRotateWriter = nil
	}
	if globalStmtRotateWriter != nil {
		err := globalStmtRotateWriter.Close()
		assert.NoError(t, err, "Failed to close existing globalStmtRotateWriter")
		globalStmtRotateWriter = nil
	}
	defer func() {
		if globalSQLRotateWriter != nil {
			err := globalSQLRotateWriter.Close()
			assert.NoError(t, err, "Failed to close globalSQLRotateWriter")
			globalSQLRotateWriter = nil
		}
		if globalStmtRotateWriter != nil {
			err := globalStmtRotateWriter.Close()
			assert.NoError(t, err, "Failed to close existing globalStmtRotateWriter")
			globalStmtRotateWriter = nil
		}
	}()
	config.Conf.Log.Path = "/"
	mission := getMission(RecordTypeSQL)
	t.Log(mission)
	err := StartRecordSql(time.Now().Format(InputTimeFormat), time.Now().Add(time.Second*2).Format(InputTimeFormat), "")
	assert.Error(t, err)
	_ = StopRecordSqlMission()

	mission = getMission(RecordTypeSQL)
	t.Log(mission)
	err = StartRecordStmt(time.Now().Format(InputTimeFormat), time.Now().Add(time.Second*2).Format(InputTimeFormat), "")
	assert.Error(t, err)
	_ = StopRecordStmtMission()
}

func TestStopRecordSql(t *testing.T) {
	// Setup test cases
	tmpDir := t.TempDir()
	tests := []struct {
		name        string
		setup       func() *RecordMission
		expectStop  bool
		expectError bool
	}{
		{
			name: "No running mission",
			setup: func() *RecordMission {
				setMission(RecordTypeSQL, nil)
				return nil
			},
			expectStop:  false,
			expectError: false,
		},
		{
			name: "Running mission",
			setup: func() *RecordMission {
				out := filepath.Join(tmpDir, "test.csv")
				f, err := os.Create(out)
				require.NoError(t, err)
				mission := &RecordMission{
					recordType: RecordTypeSQL,
					running:    true,
					cancelFunc: func() {},
					csvWriter:  csv.NewWriter(f),
					recordList: NewRecordList(),
					logger:     logrus.NewEntry(logrus.New()),
					startChan:  make(chan struct{}, 1),
				}
				close(mission.startChan)
				setMission(RecordTypeSQL, mission)
				return mission
			},
			expectStop:  true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test case
			expectedMission := tt.setup()

			// Run the function
			_ = StopRecordSqlMission()

			// Verify results
			if tt.expectStop && expectedMission != nil {
				assert.False(t, expectedMission.running, "mission should be stopped")
			}

			// Check global mission is cleared
			assert.Nil(t, getMission(RecordTypeSQL), "global mission should be nil after stop")
		})
	}
}

func TestStopRecordSqlConcurrency(t *testing.T) {
	// Set up a running mission
	tmpDir := t.TempDir()
	out := filepath.Join(tmpDir, "test.csv")
	f, err := os.Create(out)
	require.NoError(t, err)
	mission := &RecordMission{
		recordType: RecordTypeSQL,
		running:    true,
		cancelFunc: func() {},
		csvWriter:  csv.NewWriter(f),
		recordList: NewRecordList(),
		logger:     logrus.NewEntry(logrus.New()),
		startChan:  make(chan struct{}),
	}
	setMission(RecordTypeSQL, mission)
	close(mission.startChan)
	// Test concurrent stops
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = StopRecordSqlMission()
		}()
	}
	wg.Wait()

	// Verify mission was stopped and cleared
	assert.False(t, mission.running, "mission should be stopped")
	assert.Nil(t, getMission(RecordTypeSQL), "global mission should be nil after stop")
}

func TestRecordMissionStart(t *testing.T) {
	tests := []struct {
		name         string
		startTime    time.Time
		ctxCancel    bool
		expectRun    bool
		delayCheck   time.Duration // how long to wait before checking if it started
		alreadyStart bool
	}{
		{
			name:      "start immediately",
			startTime: time.Now().Add(-1 * time.Second), // in the past
			expectRun: true,
		},
		{
			name:      "start after delay",
			startTime: time.Now().Add(10 * time.Millisecond),
			expectRun: true,
		},
		{
			name:      "context canceled before start",
			startTime: time.Now().Add(100 * time.Millisecond),
			ctxCancel: true,
			expectRun: false,
		},
		{
			name:         "already started",
			startTime:    time.Now().Add(-1 * time.Second),
			alreadyStart: true,
			expectRun:    false,
		},
		{
			name:       "start with future time",
			startTime:  time.Now().Add(100 * time.Millisecond), // future time
			expectRun:  true,
			delayCheck: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tempFile, err := os.CreateTemp("", "test-recording-*.csv")
			require.NoError(t, err)
			defer func() {
				err = os.Remove(tempFile.Name())
				assert.NoError(t, err)
			}()

			mission := &RecordMission{
				recordType: RecordTypeSQL,
				startTime:  tt.startTime,
				csvWriter:  csv.NewWriter(tempFile),
				ctx:        ctx,
				cancelFunc: cancel,
				startChan:  make(chan struct{}, 1),
				recordList: NewRecordList(),
				logger:     logger,
			}

			if tt.alreadyStart {
				close(mission.startChan)
			}

			if tt.ctxCancel {
				cancel()
			}

			// Execute
			mission.start()

			// Verify
			if tt.expectRun {
				// Give some time for the goroutine to start
				delay := 20 * time.Millisecond
				if tt.delayCheck != 0 {
					delay = tt.delayCheck
				}
				time.Sleep(delay)
				assert.True(t, mission.isRunning())
			} else {
				assert.False(t, mission.isRunning())
			}

			// Cleanup
			if mission.isRunning() {
				mission.Stop()
			}
		})
	}
}

func TestRecordMissionRun(t *testing.T) {
	t.Run("normal run until context done", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithCancel(context.Background())

		tempFile, err := os.CreateTemp("", "test-recording-*.csv")
		require.NoError(t, err)
		defer func() {
			err = os.Remove(tempFile.Name())
			assert.NoError(t, err)
		}()

		mission := &RecordMission{
			recordType: RecordTypeSQL,
			csvWriter:  csv.NewWriter(tempFile),
			ctx:        ctx,
			cancelFunc: cancel,
			startChan:  make(chan struct{}, 1),
			recordList: NewRecordList(),
			logger:     logger,
			running:    true, // manually set to running
		}

		// Execute in goroutine
		go mission.run(time.Millisecond)
		close(mission.startChan)
		// Verify it's running
		assert.True(t, mission.isRunning())
		mission.writeLock.Lock()
		err = mission.csvWriter.Write([]string{"Test", "SQLRecord"})
		mission.writeLock.Unlock()
		require.NoError(t, err)
		time.Sleep(time.Millisecond * 5)
		// Check if the file has been written
		var fileWritten bool
		for i := 0; i < 10; i++ {
			bs, err := os.ReadFile(tempFile.Name())
			if err == nil && string(bs) == "Test,SQLRecord\n" {
				fileWritten = true
				break
			}
			time.Sleep(time.Millisecond * 5)
		}
		if !fileWritten {
			t.Errorf("file not written")
		}
		// Cancel the context to stop
		cancel()

		// Give some time to stop
		time.Sleep(10 * time.Millisecond)
		assert.False(t, mission.isRunning())
	})
}

func TestGetRecordState(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	tempFile, err := os.CreateTemp(tempDir, "test-recording-*.csv")
	require.NoError(t, err)
	defer func() {
		err = tempFile.Close()
		assert.NoError(t, err)
	}()

	originalMission := getMission(RecordTypeSQL)
	defer setMission(RecordTypeSQL, originalMission)

	tests := []struct {
		name          string
		setupMission  func() *RecordMission
		expectedState RecordState
	}{
		{
			name:         "no mission exists",
			setupMission: func() *RecordMission { return nil },
			expectedState: RecordState{
				Exists: false,
			},
		},
		{
			name: "mission exists but not running",
			setupMission: func() *RecordMission {
				ctx, cancel := context.WithCancel(context.Background())
				return &RecordMission{
					recordType: RecordTypeSQL,
					running:    false,
					ctx:        ctx,
					cancelFunc: cancel,
					startTime:  time.Now(),
					endTime:    time.Now().Add(1 * time.Hour),
					csvWriter:  csv.NewWriter(tempFile),
					startChan:  make(chan struct{}),
					recordList: NewRecordList(),
					logger:     logrus.NewEntry(logrus.New()),
				}
			},
			expectedState: RecordState{
				Exists:            true,
				Running:           false,
				StartTime:         time.Now().Format(ResultTimeFormat),
				EndTime:           time.Now().Add(1 * time.Hour).Format(ResultTimeFormat),
				CurrentConcurrent: 0,
			},
		},
		{
			name: "mission exists and running",
			setupMission: func() *RecordMission {
				ctx, cancel := context.WithCancel(context.Background())
				mission := &RecordMission{
					recordType: RecordTypeSQL,
					running:    true,
					ctx:        ctx,
					cancelFunc: cancel,
					startTime:  time.Now(),
					endTime:    time.Now().Add(2 * time.Hour),
					csvWriter:  csv.NewWriter(tempFile),
					startChan:  make(chan struct{}),
					recordList: NewRecordList(),
					logger:     logrus.NewEntry(logrus.New()),
				}
				mission.currentCount.Store(5)
				return mission
			},
			expectedState: RecordState{
				Exists:            true,
				Running:           true,
				StartTime:         time.Now().Format(ResultTimeFormat),
				EndTime:           time.Now().Add(2 * time.Hour).Format(ResultTimeFormat),
				CurrentConcurrent: 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test mission
			mission := tt.setupMission()
			setMission(RecordTypeSQL, mission)

			// Execute
			state := GetSqlMissionState()

			// Verify
			assert.Equal(t, tt.expectedState.Exists, state.Exists)
			if tt.expectedState.Exists {
				assert.Equal(t, tt.expectedState.Running, state.Running)
				assert.Equal(t, tt.expectedState.CurrentConcurrent, state.CurrentConcurrent)

				// For time fields, we can't compare exact values, so just check they're formatted
				assert.NotEmpty(t, state.StartTime)
				assert.NotEmpty(t, state.EndTime)
			}
		})
	}
}

func TestRecordMissionStop(t *testing.T) {
	t.Run("normal stop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		tempFile, err := os.CreateTemp("", "test-recording-*.csv")
		require.NoError(t, err)
		defer func() {
			err = os.Remove(tempFile.Name())
			assert.NoError(t, err)
		}()

		mission := &RecordMission{
			recordType: RecordTypeSQL,
			csvWriter:  csv.NewWriter(tempFile),
			ctx:        ctx,
			cancelFunc: cancel,
			startChan:  make(chan struct{}, 1),
			recordList: NewRecordList(),
			logger:     logger,
			running:    true, // manually set to running
		}

		// AddSqlRecord some test records
		now := time.Now()
		testRecord := &SQLRecord{
			SQL:             "test sql",
			IP:              "::1",
			User:            "root",
			ConnType:        HTTPType,
			QID:             0x1234,
			ReceiveTime:     now.Add(-time.Second),
			FreeTime:        now,
			QueryDuration:   time.Millisecond,
			FetchDuration:   time.Millisecond,
			GetConnDuration: time.Millisecond,
			totalDuration:   time.Second,
			mission:         mission,
		}
		ele := mission.recordList.Add(testRecord)
		require.NotNil(t, ele)
		assert.Equal(t, testRecord, ele.Value)
		close(mission.startChan)
		// Execute
		mission.Stop()

		// Verify
		assert.False(t, mission.isRunning())
		assert.True(t, mission.ctx.Err() != nil) // context should be canceled

	})

	t.Run("double stop", func(t *testing.T) {
		// Setup
		logger := logrus.NewEntry(logrus.New())
		ctx, cancel := context.WithCancel(context.Background())

		tempFile, err := os.CreateTemp("", "test-recording-*.csv")
		require.NoError(t, err)
		defer func() {
			err = os.Remove(tempFile.Name())
			assert.NoError(t, err)
		}()

		mission := &RecordMission{
			recordType: RecordTypeSQL,
			csvWriter:  csv.NewWriter(tempFile),
			ctx:        ctx,
			cancelFunc: cancel,
			startChan:  make(chan struct{}, 1),
			recordList: NewRecordList(),
			logger:     logger,
			running:    true, // manually set to running
		}
		close(mission.startChan)
		// Execute stop twice
		mission.Stop()
		mission.Stop() // should be no-op due to closeOnce

		// Verify
		assert.False(t, mission.isRunning())
	})

	t.Run("stop with write error", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithCancel(context.Background())

		// Create and immediately close a temp file to force error on write
		tempFile, err := os.CreateTemp("", "test-recording-*.csv")
		require.NoError(t, err)
		err = tempFile.Close()
		require.NoError(t, err)
		defer func() {
			err = os.Remove(tempFile.Name())
			assert.NoError(t, err)
		}()

		mission := &RecordMission{
			recordType: RecordTypeSQL,
			csvWriter:  csv.NewWriter(tempFile),
			ctx:        ctx,
			cancelFunc: cancel,
			startChan:  make(chan struct{}, 1),
			recordList: NewRecordList(),
			logger:     logger,
			running:    true, // manually set to running
		}

		// Add some test records
		now := time.Now()
		testRecord := &SQLRecord{
			SQL:             "test sql",
			IP:              "::1",
			User:            "root",
			ConnType:        HTTPType,
			QID:             0x1234,
			ReceiveTime:     now.Add(-time.Second),
			FreeTime:        now,
			QueryDuration:   time.Millisecond,
			FetchDuration:   time.Millisecond,
			GetConnDuration: time.Millisecond,
			totalDuration:   time.Second,
			mission:         mission,
		}
		ele := mission.recordList.Add(testRecord)
		require.NotNil(t, ele)
		assert.Equal(t, testRecord, ele.Value)
		close(mission.startChan)
		// Execute
		mission.Stop()

		// Verify - main thing is that it doesn't panic
		assert.False(t, mission.isRunning())
	})
}

func TestInit(t *testing.T) {
	old := config.Conf.Log.EnableSqlToCsvLogging
	defer func() {
		config.Conf.Log.EnableSqlToCsvLogging = old
	}()
	oldStmt := config.Conf.Log.EnableStmtToCsvLogging
	defer func() {
		config.Conf.Log.EnableStmtToCsvLogging = oldStmt
	}()
	config.Conf.Log.EnableSqlToCsvLogging = false
	config.Conf.Log.EnableStmtToCsvLogging = false
	err := Init()
	require.NoError(t, err)
	config.Conf.Log.EnableSqlToCsvLogging = true
	config.Conf.Log.EnableStmtToCsvLogging = true
	err = Init()
	assert.NoError(t, err)
	info := GetSqlMissionState()
	assert.Equal(t, DefaultRecordSqlEndTime, info.EndTime)
	info = GetStmtMissionState()
	assert.Equal(t, DefaultRecordSqlEndTime, info.EndTime)
	Close()
}
