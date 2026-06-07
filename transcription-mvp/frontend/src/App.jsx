import React, { useState, useRef, useCallback } from 'react';

function useWebSocketTranscript(serverUrl) {
  const [wsStatus, setWsStatus] = useState('idle');
  const [finalTranscript, setFinalTranscript] = useState('');
  const [interimTranscript, setInterimTranscript] = useState('');
  const [error, setError] = useState(null);
  const [wsLogs, setWsLogs] = useState([]);

  const wsRef = useRef(null);
  const manualCloseRef = useRef(false);

  const addWsLog = useCallback((msg) => {
    setWsLogs((prev) => [`[${new Date().toLocaleTimeString()}] ${msg}`, ...prev].slice(0, 20));
  }, []);

  const connect = useCallback(() => {
    return new Promise((resolve, reject) => {
      const wsUrl = serverUrl.replace(/^http/, 'ws') + '/ws/transcribe';
      addWsLog(`Connecting to ${wsUrl}`);
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;
      manualCloseRef.current = false;

      ws.onopen = () => {
        addWsLog('✓ Connected');
        setWsStatus('connected');
        resolve(ws);
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === 'transcript') {
            if (msg.final) {
              setFinalTranscript(prev => prev + (prev ? ' ' : '') + msg.text);
              setInterimTranscript(''); // Clear interim once text becomes final
              addWsLog(`✅ Final: ${msg.text}`);
            } else {
              setInterimTranscript(msg.text);
              addWsLog(`📝 Interim: ${msg.text}`);
            }
          } else if (msg.type === 'error') {
            setError(msg.message);
          }
        } catch (e) {
          addWsLog(`❌ Parse error: ${e.message}`);
        }
      };

      ws.onclose = () => {
        setWsStatus('idle');
        if (!manualCloseRef.current) addWsLog('❌ Connection closed unexpectedly');
      };

      ws.onerror = () => {
        setError('WebSocket Error');
        reject();
      };
    });
  }, [serverUrl, addWsLog]);

  const sendAudioChunk = useCallback((arrayBuffer) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      // SENDING RAW BINARY
      wsRef.current.send(arrayBuffer);
      return true;
    }
    return false;
  }, []);

  const disconnect = useCallback(() => {
    manualCloseRef.current = true;
    if (wsRef.current) {
      // Send JSON text command to stop
      if (wsRef.current.readyState === WebSocket.OPEN) {
        wsRef.current.send(JSON.stringify({ type: 'stop' }));
      }
      wsRef.current.close();
    }
    setWsStatus('idle');
  }, []);

  return {
    wsStatus,
    finalTranscript,
    interimTranscript,
    error,
    wsLogs,
    connect,
    disconnect,
    sendAudioChunk,
    isConnected: wsStatus === 'connected',
  };
}

// useAudioCapture remains mostly the same, but we ensure it handles the 
// binary buffer correctly from the worklet.
function useAudioCapture(onAudioChunk) {
  const [isRecording, setIsRecording] = useState(false);
  const audioContextRef = useRef(null);
  const streamRef = useRef(null);
  const workletNodeRef = useRef(null);

  const stopRecording = useCallback(async () => {
    if (workletNodeRef.current) workletNodeRef.current.port.postMessage({ type: 'stop' });
    if (streamRef.current) streamRef.current.getTracks().forEach(t => t.stop());
    if (audioContextRef.current) await audioContextRef.current.close();
    setIsRecording(false);
  }, []);

  const startRecording = useCallback(async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      streamRef.current = stream;
      const AudioContextClass = window.AudioContext || window.webkitAudioContext;
      const ctx = new AudioContextClass({ latencyHint: 'interactive' });
      audioContextRef.current = ctx;

      await ctx.audioWorklet.addModule('/pcm-worklet.js');
      const source = ctx.createMediaStreamSource(stream);
      const worklet = new AudioWorkletNode(ctx, 'pcm-worklet', { channelCount: 1 });
      workletNodeRef.current = worklet;

      worklet.port.onmessage = (e) => {
        if (e.data.type === 'pcm') {
          onAudioChunk(e.data.buffer);
        }
      };

      source.connect(worklet);
      setIsRecording(true);
      return true;
    } catch (e) {
      console.error(e);
      return false;
    }
  }, [onAudioChunk]);

  return { isRecording, startRecording, stopRecording };
}

export default function TranscriptionApp() {
  const serverUrl = window.location.hostname === 'localhost' ? 'http://localhost:8000' : 'https://sttdemo-production.up.railway.app';

  const {
    finalTranscript,
    interimTranscript,
    error,
    connect,
    disconnect,
    sendAudioChunk,
    isConnected,
    wsLogs,
  } = useWebSocketTranscript(serverUrl);

  const { isRecording, startRecording, stopRecording } = useAudioCapture(sendAudioChunk);

  const handleStart = async () => {
    await connect();
    await startRecording();
  };

  const handleStop = async () => {
    await stopRecording();
    disconnect();
  };

  return (
    <div style={{ padding: '2rem', maxWidth: '800px', margin: '0 auto', fontFamily: 'sans-serif' }}>
      <h1>Real-Time Binary Transcription</h1>
      
      <div style={{ marginBottom: '1rem', display: 'flex', gap: '10px' }}>
        {!isRecording ? (
          <button onClick={handleStart} disabled={isConnected}>Start Recording</button>
        ) : (
          <button onClick={handleStop} style={{ backgroundColor: 'red', color: 'white' }}>Stop Recording</button>
        )}
        <span>Status: {isRecording ? '🔴 Recording' : isConnected ? '🟢 Connected' : '⚪ Idle'}</span>
      </div>

      {error && <div style={{ color: 'red' }}>{error}</div>}

      <div style={{ 
        border: '1px solid #ccc', 
        padding: '1rem', 
        minHeight: '200px', 
        borderRadius: '8px',
        backgroundColor: '#f9f9f9',
        whiteSpace: 'pre-wrap' 
      }}>
        {finalTranscript}
        <span style={{ color: '#888' }}>{interimTranscript}</span>
        {!finalTranscript && !interimTranscript && <i style={{ color: '#aaa' }}>Transcript will appear here...</i>}
      </div>

      <div style={{ marginTop: '2rem', fontSize: '0.8rem', color: '#666' }}>
        <strong>Logs:</strong>
        <div style={{ height: '150px', overflowY: 'auto', background: '#eee', padding: '5px' }}>
          {wsLogs.map((log, i) => <div key={i}>{log}</div>)}
        </div>
      </div>
    </div>
  );
}