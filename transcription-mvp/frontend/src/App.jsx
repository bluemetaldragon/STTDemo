import React, { useState, useRef, useEffect, useCallback } from 'react';

// ---------- WebSocket hook with binary audio and ping ----------
function useWebSocketTranscript(serverUrl) {
  const [wsStatus, setWsStatus] = useState('idle');
  const [finalTranscript, setFinalTranscript] = useState('');
  const [interimTranscript, setInterimTranscript] = useState('');
  const [error, setError] = useState(null);
  const [wsLogs, setWsLogs] = useState([]);
  const wsRef = useRef(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimerRef = useRef(null);
  const manualCloseRef = useRef(false);
  const pingIntervalRef = useRef(null);

  const addWsLog = useCallback((msg) => {
    console.log('[WS]', msg);
    setWsLogs(prev => [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`].slice(-20));
  }, []);

  const clearReconnectTimer = useCallback(() => {
    if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
  }, []);

  const connect = useCallback(() => {
    clearReconnectTimer();
    if (wsRef.current?.readyState === WebSocket.OPEN) return Promise.resolve(wsRef.current);

    return new Promise((resolve, reject) => {
      try {
        setWsStatus('connecting');
        setError(null);
        const wsUrl = serverUrl.replace(/^http/, 'ws') + '/ws/transcribe';
        addWsLog(`Connecting to ${wsUrl}`);
        const ws = new WebSocket(wsUrl);
        ws.binaryType = 'arraybuffer';
        wsRef.current = ws;
        manualCloseRef.current = false;

        ws.onopen = () => {
          addWsLog('✓ WebSocket connected');
          setWsStatus('connected');
          reconnectAttemptsRef.current = 0;
          resolve(ws);
          if (pingIntervalRef.current) clearInterval(pingIntervalRef.current);
          pingIntervalRef.current = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify({ type: 'ping' }));
            }
          }, 5000);
        };

        ws.onmessage = (event) => {
          if (typeof event.data === 'string') {
            try {
              const msg = JSON.parse(event.data);
              if (msg.type === 'transcript' && msg.text) {
                addWsLog(`📝 Transcript: "${msg.text}" ${msg.final ? '(final)' : '(interim)'}`);
                if (msg.final === true) {
                  setFinalTranscript(prev => prev + (prev ? ' ' : '') + msg.text);
                  setInterimTranscript('');
                } else {
                  setInterimTranscript(msg.text);
                }
              } else if (msg.type === 'error') {
                addWsLog(`❌ Error: ${msg.message}`);
                setError(msg.message);
              } else if (msg.type === 'deepgram_ready') {
                addWsLog('🎙️ Deepgram ready');
              }
            } catch (e) {
              addWsLog(`❌ Parse error: ${e.message}`);
            }
          }
        };

        ws.onerror = () => {
          addWsLog('❌ WebSocket error');
          setWsStatus('error');
          setError('WebSocket error');
        };

        ws.onclose = (event) => {
          if (pingIntervalRef.current) clearInterval(pingIntervalRef.current);
          addWsLog(`❌ Closed (code=${event.code})`);
          wsRef.current = null;
          setWsStatus('idle');
          if (!manualCloseRef.current && reconnectAttemptsRef.current < 3) {
            reconnectAttemptsRef.current++;
            const delay = Math.min(1000 * Math.pow(2, reconnectAttemptsRef.current - 1), 5000);
            addWsLog(`Reconnecting in ${delay}ms...`);
            reconnectTimerRef.current = setTimeout(() => connect().catch(() => {}), delay);
          }
        };
      } catch (e) {
        addWsLog(`❌ Connection exception: ${e.message}`);
        setWsStatus('error');
        setError(e.message);
        reject(e);
      }
    });
  }, [serverUrl, addWsLog, clearReconnectTimer]);

  const sendAudioChunk = useCallback((arrayBuffer) => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    ws.send(arrayBuffer);
    return true;
  }, []);

  const disconnect = useCallback(() => {
    clearReconnectTimer();
    manualCloseRef.current = true;
    if (pingIntervalRef.current) clearInterval(pingIntervalRef.current);
    if (wsRef.current) {
      try {
        if (wsRef.current.readyState === WebSocket.OPEN) {
          wsRef.current.send(JSON.stringify({ type: 'stop' }));
        }
        wsRef.current.close();
      } catch (e) {}
    }
    wsRef.current = null;
    setWsStatus('idle');
  }, [clearReconnectTimer]);

  const resetTranscript = useCallback(() => {
    setFinalTranscript('');
    setInterimTranscript('');
  }, []);

  useEffect(() => {
    return () => {
      manualCloseRef.current = true;
      clearReconnectTimer();
      if (pingIntervalRef.current) clearInterval(pingIntervalRef.current);
      wsRef.current?.close();
    };
  }, [clearReconnectTimer]);

  const displayTranscript = finalTranscript + (interimTranscript ? ' ' + interimTranscript : '');

  return {
    wsStatus,
    transcript: displayTranscript,
    error,
    wsLogs,
    connect,
    disconnect,
    sendAudioChunk,
    isConnected: wsStatus === 'connected',
    resetTranscript,
  };
}

function useAudioCapture(onAudioChunk) {
  const [isRecording, setIsRecording] = useState(false);
  const [debugLogs, setDebugLogs] = useState([]);
  const mediaRecorderRef = useRef(null);
  const streamRef = useRef(null);

  const addLog = useCallback((msg) => {
    console.log('[AUDIO]', msg);
    setDebugLogs(prev => [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`].slice(-20));
  }, []);

  const startRecording = useCallback(async () => {
    addLog('Requesting microphone');
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      streamRef.current = stream;
      const tracks = stream.getTracks();
      addLog(`✓ Microphone access granted, tracks: ${tracks.length}, track state: ${tracks[0]?.readyState}`);

      // Check supported MIME types
      const mimeTypes = ['audio/webm', 'audio/mp4', 'audio/webm;codecs=opus'];
      let selectedMime = null;
      for (const mime of mimeTypes) {
        if (MediaRecorder.isTypeSupported(mime)) {
          selectedMime = mime;
          break;
        }
      }
      addLog(`Using MIME type: ${selectedMime || 'default'}`);

      const mediaRecorder = new MediaRecorder(stream, { mimeType: selectedMime });
      mediaRecorderRef.current = mediaRecorder;

      // Log recorder errors
      mediaRecorder.onerror = (event) => {
        addLog(`❌ MediaRecorder error: ${event.error?.message || 'unknown'}`);
      };

      mediaRecorder.ondataavailable = (event) => {
        addLog(`📦 Data available: ${event.data.size} bytes`);
        if (event.data.size > 0) {
          event.data.arrayBuffer().then(buffer => {
            addLog(`Sending chunk to WebSocket, size: ${buffer.byteLength}`);
            const sent = onAudioChunk(buffer);
            addLog(`Chunk sent: ${sent ? 'yes' : 'no (WebSocket not open)'}`);
          }).catch(err => addLog(`ArrayBuffer error: ${err.message}`));
        }
      };

      // Start recording – request data every 500ms
      mediaRecorder.start(500);
      addLog(`MediaRecorder started, state: ${mediaRecorder.state}`);

      // If after 2 seconds no data arrives, log a warning
      setTimeout(() => {
        if (mediaRecorder.state === 'recording') {
          addLog('⚠️ Still recording but no data available event fired yet');
        }
      }, 2000);

      setIsRecording(true);
      addLog('✓ Recording started (MediaRecorder)');
      return true;
    } catch (error) {
      addLog(`❌ ${error.name}: ${error.message}`);
      return false;
    }
  }, [onAudioChunk, addLog]);

  const stopRecording = useCallback(() => {
    addLog('Stopping recording');
    if (mediaRecorderRef.current && mediaRecorderRef.current.state !== 'inactive') {
      mediaRecorderRef.current.stop();
      addLog('MediaRecorder stopped');
    }
    if (streamRef.current) {
      streamRef.current.getTracks().forEach(track => track.stop());
    }
    setIsRecording(false);
    addLog('✓ Stopped');
  }, [addLog]);

  useEffect(() => {
    return () => {
      if (mediaRecorderRef.current && mediaRecorderRef.current.state !== 'inactive') {
        mediaRecorderRef.current.stop();
      }
      if (streamRef.current) {
        streamRef.current.getTracks().forEach(track => track.stop());
      }
    };
  }, []);

  return { isRecording, startRecording, stopRecording, debugLogs };
}

// ---------- Main UI component ----------
export default function TranscriptionApp() {
  const getServerUrl = () => {
    const hostname = window.location.hostname;
    if (hostname.includes('vercel.app')) return 'https://sttdemo-production.up.railway.app';
    return 'http://localhost:8000';
  };
  const serverUrl = getServerUrl();

  const { transcript, error, connect, disconnect, sendAudioChunk, isConnected, wsLogs, resetTranscript } =
    useWebSocketTranscript(serverUrl);
  const { isRecording, startRecording, stopRecording, debugLogs } = useAudioCapture(sendAudioChunk);
  const [isBusy, setIsBusy] = useState(false);
  const transcriptRef = useRef(null);
  const [copyStatus, setCopyStatus] = useState('');

  useEffect(() => {
    transcriptRef.current?.scrollTo({ top: transcriptRef.current.scrollHeight, behavior: 'smooth' });
  }, [transcript]);

  const handleStart = async () => {
    if (isRecording || isBusy) return;
    setIsBusy(true);
    try {
      resetTranscript();
      await connect();
      const ok = await startRecording();
      if (!ok) disconnect();
    } catch (e) {
      disconnect();
    } finally {
      setIsBusy(false);
    }
  };

  const handleStop = async () => {
    if (isBusy) return;
    setIsBusy(true);
    try {
      await stopRecording();
      disconnect();
    } finally {
      setIsBusy(false);
    }
  };

  const copyTranscript = async () => {
    try {
      await navigator.clipboard.writeText(transcript);
      setCopyStatus('Copied!');
      setTimeout(() => setCopyStatus(''), 2000);
    } catch {
      setCopyStatus('Copy failed');
    }
  };

  return (
    <div style={{ minHeight: '100vh', backgroundColor: '#f9fafb', fontFamily: 'system-ui' }}>
      <div style={{ backgroundColor: '#fff', borderBottom: '1px solid #e5e7eb', padding: '1rem' }}>
        <h1 style={{ margin: 0, fontSize: '1.5rem', fontWeight: 600 }}>Real-Time Transcription</h1>
        <p style={{ margin: 0, fontSize: '0.875rem', color: '#6b7280' }}>Binary audio streaming (MediaRecorder)</p>
      </div>

      <div style={{ maxWidth: '1000px', margin: '0 auto', padding: '1.5rem 1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', background: '#f3f4f6', borderRadius: '0.5rem', padding: '1rem', marginBottom: '1.5rem' }}>
          <div style={{ width: '12px', height: '12px', borderRadius: '50%', backgroundColor: isRecording ? '#ef4444' : isConnected ? '#10b981' : '#6b7280' }} />
          <span>{isRecording ? 'Recording...' : isConnected ? 'Connected' : 'Ready'}</span>
          {error && <span style={{ marginLeft: 'auto', color: '#dc2626' }}>{error}</span>}
        </div>

        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1.5rem', flexWrap: 'wrap' }}>
          <button onClick={handleStart} disabled={isRecording || isBusy} style={{ padding: '0.625rem 1rem', backgroundColor: (isRecording || isBusy) ? '#d1d5db' : '#3b82f6', color: '#fff', border: 'none', borderRadius: '0.375rem', cursor: (isRecording || isBusy) ? 'not-allowed' : 'pointer', flex: 1 }}>Start</button>
          <button onClick={handleStop} disabled={!isRecording || isBusy} style={{ padding: '0.625rem 1rem', backgroundColor: (!isRecording || isBusy) ? '#d1d5db' : '#ef4444', color: '#fff', border: 'none', borderRadius: '0.375rem', cursor: (!isRecording || isBusy) ? 'not-allowed' : 'pointer', flex: 1 }}>Stop</button>
          <button onClick={copyTranscript} disabled={!transcript} style={{ padding: '0.625rem 1rem', backgroundColor: !transcript ? '#d1d5db' : '#10b981', color: '#fff', border: 'none', borderRadius: '0.375rem', cursor: !transcript ? 'not-allowed' : 'pointer', flex: 1 }}>{copyStatus || 'Copy'}</button>
        </div>

        <div style={{ marginBottom: '1rem', background: '#1f2937', borderRadius: '0.375rem', padding: '0.75rem', fontSize: '0.7rem', fontFamily: 'monospace', color: '#fbbf24', maxHeight: '120px', overflowY: 'auto' }}>
          <div style={{ fontWeight: 'bold' }}>WEBSOCKET LOG</div>
          {wsLogs.map((log, i) => <div key={i}>{log}</div>)}
        </div>
        <div style={{ marginBottom: '1rem', background: '#1f2937', borderRadius: '0.375rem', padding: '0.75rem', fontSize: '0.7rem', fontFamily: 'monospace', color: '#10b981', maxHeight: '120px', overflowY: 'auto' }}>
          <div style={{ fontWeight: 'bold' }}>AUDIO LOG</div>
          {debugLogs.map((log, i) => <div key={i}>{log}</div>)}
        </div>

        <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: '0.5rem' }}>
          <div style={{ padding: '0.75rem 1rem', background: '#f3f4f6', borderBottom: '1px solid #e5e7eb', fontWeight: 500 }}>Transcript</div>
          <div ref={transcriptRef} style={{ padding: '1rem', minHeight: '200px', maxHeight: '400px', overflowY: 'auto', whiteSpace: 'pre-wrap' }}>
            {transcript || <span style={{ color: '#9ca3af' }}>Start recording...</span>}
          </div>
        </div>
      </div>
    </div>
  );
}