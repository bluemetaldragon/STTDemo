import React, { useState, useRef, useEffect, useCallback } from 'react';

function useWebSocketTranscript(serverUrl) {
  const [wsStatus, setWsStatus] = useState('idle');
  const [transcript, setTranscript] = useState('');
  const [error, setError] = useState(null);
  const [wsLogs, setWsLogs] = useState([]);

  const wsRef = useRef(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimerRef = useRef(null);
  const manualCloseRef = useRef(false);

  const addWsLog = useCallback((msg) => {
    console.log('[WS]', msg);
    setWsLogs((prev) =>
      [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`].slice(-20)
    );
  }, []);

  const clearReconnectTimer = useCallback(() => {
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
  }, []);

  const connect = useCallback(() => {
    clearReconnectTimer();

    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      return Promise.resolve(wsRef.current);
    }

    return new Promise((resolve, reject) => {
      try {
        setWsStatus('connecting');
        setError(null);

        const wsUrl = serverUrl.replace(/^http/, 'ws') + '/ws/transcribe';
        addWsLog(`Connecting to ${wsUrl}`);

        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;
        manualCloseRef.current = false;

        ws.onopen = () => {
          addWsLog('✓ WebSocket connected');
          setWsStatus('connected');
          reconnectAttemptsRef.current = 0;
          resolve(ws);
        };

        ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);

            if (msg.type === 'transcript' && msg.text) {
              addWsLog(`📝 Transcript: "${msg.text}" ${msg.final ? '(final)' : '(interim)'}`);
              setTranscript((prev) => prev + (prev ? ' ' : '') + msg.text);
            } else if (msg.type === 'error') {
              const message =
                typeof msg.message === 'string'
                  ? msg.message
                  : JSON.stringify(msg.message);
              addWsLog(`❌ Backend error: ${message}`);
              setError(message);
            } else if (msg.type === 'speech_started') {
              addWsLog('🎤 Speech detected');
            } else if (msg.type === 'utterance_end') {
              addWsLog('⏹ Utterance ended');
            } else if (msg.type === 'metadata') {
              addWsLog(`ℹ️ Metadata received${msg.request_id ? ` (${msg.request_id})` : ''}`);
            }
          } catch (e) {
            addWsLog(`❌ Message parse error: ${e.message}`);
          }
        };

        ws.onerror = () => {
          addWsLog('❌ WebSocket error');
          setWsStatus('error');
          setError('WebSocket error');
        };

        ws.onclose = (event) => {
          const { code, reason, wasClean } = event;
          addWsLog(`❌ WebSocket closed (code=${code}, clean=${wasClean}, reason=${reason || 'n/a'})`);
          wsRef.current = null;
          setWsStatus('idle');

          if (!manualCloseRef.current) {
            if (reconnectAttemptsRef.current < 3) {
              reconnectAttemptsRef.current += 1;
              const delay = Math.min(1000 * Math.pow(2, reconnectAttemptsRef.current - 1), 5000);
              addWsLog(`Reconnecting in ${delay}ms...`);
              reconnectTimerRef.current = setTimeout(() => {
                connect().catch(() => {});
              }, delay);
            }
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
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    const uint8 = new Uint8Array(arrayBuffer);
    let binary = '';
    const chunkSize = 0x8000;

    for (let i = 0; i < uint8.length; i += chunkSize) {
      const sub = uint8.subarray(i, i + chunkSize);
      binary += String.fromCharCode.apply(null, sub);
    }

    const base64 = btoa(binary);
    ws.send(JSON.stringify({ type: 'audio', data: base64 }));
    return true;
  }, []);

  const disconnect = useCallback(() => {
    clearReconnectTimer();
    manualCloseRef.current = true;

    const ws = wsRef.current;
    if (ws) {
      try {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'stop' }));
        }
        ws.close();
      } catch (e) {
        console.warn('Error closing websocket', e);
      }
    }

    wsRef.current = null;
    setWsStatus('idle');
  }, [clearReconnectTimer]);

  useEffect(() => {
    return () => {
      manualCloseRef.current = true;
      clearReconnectTimer();
      if (wsRef.current) {
        try {
          wsRef.current.close();
        } catch (_) {}
      }
    };
  }, [clearReconnectTimer]);

  return {
    wsStatus,
    transcript,
    error,
    wsLogs,
    connect,
    disconnect,
    sendAudioChunk,
    isConnected: wsStatus === 'connected',
    setTranscript,
  };
}

function useAudioCapture(onAudioChunk) {
  const [isRecording, setIsRecording] = useState(false);
  const [debugLogs, setDebugLogs] = useState([]);

  const audioContextRef = useRef(null);
  const streamRef = useRef(null);
  const workletNodeRef = useRef(null);
  const sourceRef = useRef(null);
  const isStartingRef = useRef(false);

  const addLog = useCallback((msg) => {
    console.log('[AUDIO]', msg);
    setDebugLogs((prev) =>
      [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`].slice(-20)
    );
  }, []);

  const stopRecording = useCallback(async () => {
    addLog('Stopping recording');

    try {
      if (workletNodeRef.current) {
        try {
          workletNodeRef.current.port.postMessage({ type: 'stop' });
        } catch (_) {}
        try {
          workletNodeRef.current.disconnect();
        } catch (_) {}
        workletNodeRef.current = null;
      }

      if (sourceRef.current) {
        try {
          sourceRef.current.disconnect();
        } catch (_) {}
        sourceRef.current = null;
      }

      if (streamRef.current) {
        streamRef.current.getTracks().forEach((track) => track.stop());
        streamRef.current = null;
      }

      if (audioContextRef.current) {
        try {
          await audioContextRef.current.close();
        } catch (_) {}
        audioContextRef.current = null;
      }
    } finally {
      setIsRecording(false);
      isStartingRef.current = false;
      addLog('✓ Recording stopped');
    }
  }, [addLog]);

  const startRecording = useCallback(async () => {
    if (isRecording || isStartingRef.current) {
      addLog('Recording already active or starting');
      return true;
    }

    isStartingRef.current = true;
    addLog('Requesting microphone');

    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          channelCount: 1,
          echoCancellation: false,
          noiseSuppression: false,
          autoGainControl: false,
        },
        video: false,
      });

      streamRef.current = stream;
      addLog('✓ Microphone access granted');

      const AudioContextClass = window.AudioContext || window.webkitAudioContext;
      if (!AudioContextClass) {
        throw new Error('AudioContext is not supported in this browser');
      }

      const audioContext = new AudioContextClass({
        latencyHint: 'interactive',
      });
      audioContextRef.current = audioContext;

      addLog(`✓ AudioContext created (${audioContext.sampleRate} Hz)`);

      if (audioContext.state === 'suspended') {
        await audioContext.resume();
        addLog('✓ AudioContext resumed');
      }

      await audioContext.audioWorklet.addModule('/pcm-worklet.js');
      addLog('✓ AudioWorklet module loaded');

      const source = audioContext.createMediaStreamSource(stream);
      sourceRef.current = source;

      const workletNode = new AudioWorkletNode(audioContext, 'pcm-worklet', {
        numberOfInputs: 1,
        numberOfOutputs: 0,
        channelCount: 1,
        channelCountMode: 'explicit',
        channelInterpretation: 'speakers',
      });
      workletNodeRef.current = workletNode;

      let sentFirstChunk = false;

      workletNode.port.onmessage = (event) => {
        const data = event.data;

        if (data?.type === 'log') {
          addLog(data.message);
          return;
        }

        if (data?.type === 'pcm' && data.buffer) {
          const sent = onAudioChunk(data.buffer);
          if (!sentFirstChunk && sent) {
            sentFirstChunk = true;
            addLog(`✓ First PCM chunk sent (${data.byteLength} bytes)`);
          }
        }
      };

      source.connect(workletNode);

      setIsRecording(true);
      addLog('✓ Recording started (mono PCM16 @ 16kHz)');
      isStartingRef.current = false;
      return true;
    } catch (error) {
      const errMsg = `❌ ${error.name || 'Error'}: ${error.message}`;
      addLog(errMsg);
      console.error(errMsg, error);
      await stopRecording();
      return false;
    }
  }, [isRecording, onAudioChunk, addLog, stopRecording]);

  useEffect(() => {
    return () => {
      stopRecording().catch(() => {});
    };
  }, [stopRecording]);

  return {
    isRecording,
    startRecording,
    stopRecording,
    debugLogs,
  };
}

export default function TranscriptionApp() {
  const getServerUrl = () => {
    const hostname = window.location.hostname;
    if (hostname.includes('vercel.app')) {
      return 'https://sttdemo-production.up.railway.app';
    }
    return 'http://localhost:8000';
  };

  const serverUrl = getServerUrl();

  const {
    transcript,
    error,
    connect,
    disconnect,
    sendAudioChunk,
    isConnected,
    wsLogs,
    setTranscript,
  } = useWebSocketTranscript(serverUrl);

  const { isRecording, startRecording, stopRecording, debugLogs } =
    useAudioCapture(sendAudioChunk);

  const transcriptRef = useRef(null);
  const [copyStatus, setCopyStatus] = useState('');
  const [isBusy, setIsBusy] = useState(false);

  useEffect(() => {
    if (transcriptRef.current) {
      transcriptRef.current.scrollTop = transcriptRef.current.scrollHeight;
    }
  }, [transcript]);

  const handleStartRecording = async () => {
    if (isRecording || isBusy) return;

    setIsBusy(true);
    try {
      setTranscript('');
      await connect();
      const ok = await startRecording();
      if (!ok) {
        disconnect();
      }
    } catch (e) {
      console.error('Start flow failed', e);
      disconnect();
    } finally {
      setIsBusy(false);
    }
  };

  const handleStopRecording = async () => {
    if (isBusy) return;

    setIsBusy(true);
    try {
      await stopRecording();
      disconnect();
    } finally {
      setIsBusy(false);
    }
  };

  const handleCopyTranscript = async () => {
    try {
      await navigator.clipboard.writeText(transcript);
      setCopyStatus('Copied!');
      setTimeout(() => setCopyStatus(''), 2000);
    } catch (_) {
      setCopyStatus('Copy failed');
    }
  };

  return (
    <div style={{ minHeight: '100vh', backgroundColor: '#f9fafb', fontFamily: 'system-ui, sans-serif' }}>
      <div style={{ backgroundColor: '#ffffff', borderBottom: '1px solid #e5e7eb', padding: '1rem' }}>
        <h1 style={{ margin: '0 0 0.5rem 0', fontSize: '1.5rem', fontWeight: 600, color: '#111827' }}>
          Real-Time Transcription
        </h1>
        <p style={{ margin: 0, fontSize: '0.875rem', color: '#6b7280' }}>
          Record and transcribe conversations
        </p>
      </div>

      <div style={{ maxWidth: '1000px', margin: '0 auto', padding: '1.5rem 1rem' }}>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem',
            padding: '1rem',
            backgroundColor: '#f3f4f6',
            borderRadius: '0.5rem',
            marginBottom: '1.5rem',
          }}
        >
          <div
            style={{
              width: '12px',
              height: '12px',
              borderRadius: '50%',
              backgroundColor: isRecording ? '#ef4444' : isConnected ? '#10b981' : '#6b7280',
              animation: isRecording ? 'pulse 2s infinite' : 'none',
            }}
          />
          <span style={{ fontSize: '0.875rem', fontWeight: 500, color: '#374151' }}>
            {isRecording ? 'Recording...' : isConnected ? 'Connected' : 'Ready'}
          </span>
          {error && (
            <span style={{ marginLeft: 'auto', fontSize: '0.875rem', color: '#dc2626' }}>
              {error}
            </span>
          )}
        </div>

        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1.5rem', flexWrap: 'wrap' }}>
          <button
            onClick={handleStartRecording}
            disabled={isRecording || isBusy}
            style={{
              padding: '0.625rem 1rem',
              backgroundColor: isRecording || isBusy ? '#d1d5db' : '#3b82f6',
              color: '#ffffff',
              border: 'none',
              borderRadius: '0.375rem',
              cursor: isRecording || isBusy ? 'not-allowed' : 'pointer',
              flex: 1,
              minWidth: '140px',
            }}
          >
            {isBusy && !isRecording ? 'Starting...' : isRecording ? 'Recording' : 'Start Recording'}
          </button>

          <button
            onClick={handleStopRecording}
            disabled={!isRecording || isBusy}
            style={{
              padding: '0.625rem 1rem',
              backgroundColor: !isRecording || isBusy ? '#d1d5db' : '#ef4444',
              color: '#ffffff',
              border: 'none',
              borderRadius: '0.375rem',
              cursor: !isRecording || isBusy ? 'not-allowed' : 'pointer',
              flex: 1,
              minWidth: '140px',
            }}
          >
            {isBusy && isRecording ? 'Stopping...' : 'Stop Recording'}
          </button>

          <button
            onClick={handleCopyTranscript}
            disabled={!transcript}
            style={{
              padding: '0.625rem 1rem',
              backgroundColor: !transcript ? '#d1d5db' : '#10b981',
              color: '#ffffff',
              border: 'none',
              borderRadius: '0.375rem',
              cursor: !transcript ? 'not-allowed' : 'pointer',
              flex: 1,
              minWidth: '140px',
            }}
          >
            {copyStatus || 'Copy'}
          </button>
        </div>

        <div
          style={{
            marginBottom: '1.5rem',
            padding: '0.75rem',
            backgroundColor: '#1f2937',
            borderRadius: '0.375rem',
            fontSize: '0.7rem',
            fontFamily: 'monospace',
            color: '#fbbf24',
            maxHeight: '120px',
            overflowY: 'auto',
          }}
        >
          <div style={{ marginBottom: '0.5rem', fontWeight: 'bold' }}>WEBSOCKET LOG</div>
          {wsLogs.map((log, i) => (
            <div key={i}>{log}</div>
          ))}
        </div>

        <div
          style={{
            marginBottom: '1.5rem',
            padding: '0.75rem',
            backgroundColor: '#1f2937',
            borderRadius: '0.375rem',
            fontSize: '0.7rem',
            fontFamily: 'monospace',
            color: '#10b981',
            maxHeight: '120px',
            overflowY: 'auto',
          }}
        >
          <div style={{ marginBottom: '0.5rem', fontWeight: 'bold' }}>AUDIO LOG</div>
          {debugLogs.map((log, i) => (
            <div key={i}>{log}</div>
          ))}
        </div>

        <div style={{ backgroundColor: '#ffffff', border: '1px solid #e5e7eb', borderRadius: '0.5rem' }}>
          <div
            style={{
              padding: '0.75rem 1rem',
              backgroundColor: '#f3f4f6',
              borderBottom: '1px solid #e5e7eb',
              fontSize: '0.875rem',
              fontWeight: 500,
              color: '#6b7280',
            }}
          >
            Transcript
          </div>
          <div
            ref={transcriptRef}
            style={{
              padding: '1rem',
              minHeight: '200px',
              maxHeight: '400px',
              overflowY: 'auto',
              fontSize: '0.95rem',
              lineHeight: 1.6,
              color: '#111827',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              userSelect: 'text',
            }}
          >
            {transcript || (
              <span style={{ color: '#9ca3af', fontStyle: 'italic' }}>
                Start recording to see transcript appear here...
              </span>
            )}
          </div>
        </div>
      </div>

      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }
      `}</style>
    </div>
  );
}