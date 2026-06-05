import React, { useState, useRef, useEffect, useCallback } from 'react';

function useAudioCapture() {
  const [isRecording, setIsRecording] = useState(false);
  const [debugLogs, setDebugLogs] = useState([]);
  const audioContextRef = useRef(null);
  const streamRef = useRef(null);
  const processorRef = useRef(null);

  const addLog = (msg) => {
    console.log('[AUDIO]', msg);
    setDebugLogs(prev => [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`].slice(-15));
  };

  const startRecording = useCallback(async () => {
    addLog('startRecording called');
    try {
      if (streamRef.current) streamRef.current.getTracks().forEach(track => track.stop());
      if (audioContextRef.current && audioContextRef.current.state !== 'closed') await audioContextRef.current.close();

      addLog('Requesting microphone access...');
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      addLog('✓ Microphone access granted');
      streamRef.current = stream;

      addLog('Creating audio context...');
      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      audioContextRef.current = audioContext;
      addLog(`✓ Audio context created (${audioContext.sampleRate}Hz)`);

      let source;
      if (typeof audioContext.createMediaStreamSource === 'function') {
        source = audioContext.createMediaStreamSource(stream);
        addLog('✓ Using createMediaStreamSource');
      } else {
        source = audioContext.createMediaStreamAudioSource(stream);
        addLog('✓ Using createMediaStreamAudioSource');
      }

      const processor = audioContext.createScriptProcessor(4096, 1, 1);
      processorRef.current = processor;
      source.connect(processor);
      processor.connect(audioContext.destination);
      addLog('✓ Audio chain connected');

      const audioChunks = [];
      let sendCount = 0;

      processor.onaudioprocess = (event) => {
        try {
          if (!event?.inputData?.[0]) return;
          const pcm16 = float32ToPcm16(event.inputData[0]);
          audioChunks.push(pcm16);

          if (audioChunks.length >= 2) {
            const combined = concatenateArrays(audioChunks);
            if (window.wsRef && window.wsRef.readyState === WebSocket.OPEN) {
              sendCount++;
              const base64 = btoa(String.fromCharCode(...new Uint8Array(combined)));
              window.wsRef.send(JSON.stringify({ type: 'audio', data: base64 }));
              if (sendCount % 10 === 0) addLog(`✓ Sent ${sendCount} chunks`);
            }
            audioChunks.length = 0;
          }
        } catch (err) {
          addLog(`Error: ${err.message}`);
        }
      };

      setIsRecording(true);
      addLog('✓ Recording started');
      return true;
    } catch (error) {
      addLog(`❌ Error: ${error.name} - ${error.message}`);
      return false;
    }
  }, []);

  const stopRecording = useCallback(() => {
    addLog('stopRecording called');
    if (processorRef.current) {
      processorRef.current.disconnect();
      processorRef.current.onaudioprocess = null;
      processorRef.current = null;
    }
    if (streamRef.current) {
      streamRef.current.getTracks().forEach(track => track.stop());
      streamRef.current = null;
    }
    if (audioContextRef.current && audioContextRef.current.state !== 'closed') {
      audioContextRef.current.close();
      audioContextRef.current = null;
    }
    setIsRecording(false);
    addLog('✓ Stopped');
  }, []);

  return { isRecording, startRecording, stopRecording, debugLogs };
}

function useWebSocketTranscript(serverUrl) {
  const [wsStatus, setWsStatus] = useState('idle');
  const [transcript, setTranscript] = useState('');
  const [error, setError] = useState(null);
  const [wsLogs, setWsLogs] = useState([]);
  const wsRef = useRef(null);
  const reconnectAttemptsRef = useRef(0);

  const addWsLog = (msg) => {
    console.log('[WS]', msg);
    setWsLogs(prev => [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`].slice(-10));
  };

  const connect = useCallback(() => {
    try {
      setWsStatus('connecting');
      setError(null);
      
      const wsUrl = serverUrl.replace('http', 'ws') + '/ws/transcribe';
      console.log('[WS] serverUrl input:', serverUrl);
      console.log('[WS] constructing wsUrl:', wsUrl);
      addWsLog('Connecting to ' + wsUrl);

      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;
      window.wsRef = ws;
      
      console.log('[WS] WebSocket object created, url:', wsUrl);

      ws.onopen = () => {
        addWsLog('✓ WebSocket CONNECTED');
        setWsStatus('connected');
        reconnectAttemptsRef.current = 0;
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === 'transcript') {
            addWsLog(`📝 Received: "${msg.text}"`);
            setTranscript(prev => prev + (prev && msg.text ? ' ' : '') + msg.text);
          }
        } catch (e) {
          console.error('[WS] Parse error:', e);
        }
      };

      ws.onerror = (event) => {
        console.error('[WS] WebSocket error event:', event);
        addWsLog(`❌ Connection error`);
        setWsStatus('error');
        setError('WebSocket error');
      };

      ws.onclose = () => {
        addWsLog('❌ WebSocket CLOSED');
        setWsStatus('idle');
        if (reconnectAttemptsRef.current < 5) {
          reconnectAttemptsRef.current += 1;
          const delay = Math.min(1000 * Math.pow(2, reconnectAttemptsRef.current - 1), 30000);
          addWsLog(`Reconnecting in ${delay}ms...`);
          setTimeout(connect, delay);
        }
      };
    } catch (e) {
      console.error('[WS] Exception:', e);
      addWsLog(`Connection error: ${e.message}`);
      setWsStatus('error');
      setError(e.message);
    }
  }, [serverUrl]);

  const disconnect = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close();
    }
    window.wsRef = null;
  }, []);

  return { wsStatus, transcript, error, connect, disconnect, isConnected: wsStatus === 'connected', wsLogs };
}

function float32ToPcm16(float32Array) {
  const pcm16 = new Int16Array(float32Array.length);
  for (let i = 0; i < float32Array.length; i++) {
    const s = Math.max(-1, Math.min(1, float32Array[i]));
    pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
  }
  return pcm16.buffer;
}

function concatenateArrays(arrays) {
  let totalLength = arrays.reduce((sum, arr) => sum + new Uint8Array(arr).length, 0);
  let result = new Uint8Array(totalLength);
  let offset = 0;
  arrays.forEach(arr => {
    result.set(new Uint8Array(arr), offset);
    offset += new Uint8Array(arr).length;
  });
  return result.buffer;
}

export default function TranscriptionApp() {
  // Dynamically construct backend URL for Codespaces or localhost
  const getServerUrl = () => {
    const hostname = window.location.hostname;
    const protocol = window.location.protocol;
    
    if (hostname.includes('vercel.app')) {
      return 'https://sttdemo-production.up.railway.app';
    }
    
    console.log('[APP] Using localhost');
    return 'http://localhost:8000';
  };

  const serverUrl = getServerUrl();
  console.log('[APP] Final serverUrl:', serverUrl);
  
  const { isRecording, startRecording, stopRecording, debugLogs } = useAudioCapture();
  const { wsStatus, transcript, error, connect, disconnect, isConnected, wsLogs } = useWebSocketTranscript(serverUrl);
  
  const transcriptRef = useRef(null);
  const [copyStatus, setCopyStatus] = useState('');

  useEffect(() => {
    if (transcriptRef.current) transcriptRef.current.scrollTop = transcriptRef.current.scrollHeight;
  }, [transcript]);

  const handleStartRecording = async () => {
    if (!isConnected) {
      connect();
      setTimeout(() => startRecording(), 1500);
    } else {
      await startRecording();
    }
  };

  const handleStopRecording = () => {
    stopRecording();
    disconnect();
  };

  const handleCopyTranscript = async () => {
    try {
      await navigator.clipboard.writeText(transcript);
      setCopyStatus('Copied!');
      setTimeout(() => setCopyStatus(''), 2000);
    } catch (err) {
      setCopyStatus('Copy failed');
    }
  };

  return (
    <div style={{ minHeight: '100vh', backgroundColor: '#f9fafb', fontFamily: 'system-ui' }}>
      <div style={{ backgroundColor: '#ffffff', borderBottom: '1px solid #e5e7eb', padding: '1rem' }}>
        <h1 style={{ margin: '0 0 0.5rem 0', fontSize: '1.5rem', fontWeight: '600', color: '#111827' }}>Real-Time Transcription</h1>
        <p style={{ margin: 0, fontSize: '0.875rem', color: '#6b7280' }}>Record and transcribe conversations</p>
      </div>

      <div style={{ maxWidth: '100%', margin: '0 auto', padding: '1.5rem 1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', padding: '1rem', backgroundColor: '#f3f4f6', borderRadius: '0.5rem', marginBottom: '1.5rem' }}>
          <div style={{ width: '12px', height: '12px', borderRadius: '50%', backgroundColor: isRecording ? '#ef4444' : isConnected ? '#10b981' : '#6b7280', animation: isRecording ? 'pulse 2s infinite' : 'none' }} />
          <span style={{ fontSize: '0.875rem', fontWeight: '500', color: '#374151' }}>{isRecording ? 'Recording...' : isConnected ? 'Connected' : 'Ready'}</span>
          {error && <span style={{ marginLeft: 'auto', fontSize: '0.875rem', color: '#dc2626' }}>{error}</span>}
        </div>

        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1.5rem', flexWrap: 'wrap' }}>
          <button onClick={handleStartRecording} disabled={isRecording} style={{ padding: '0.625rem 1rem', backgroundColor: isRecording ? '#d1d5db' : '#3b82f6', color: '#ffffff', border: 'none', borderRadius: '0.375rem', cursor: isRecording ? 'not-allowed' : 'pointer', flex: 1, minWidth: '120px' }}>
            {isRecording ? '🔴 Recording' : '▶ Start Recording'}
          </button>
          <button onClick={handleStopRecording} disabled={!isRecording} style={{ padding: '0.625rem 1rem', backgroundColor: !isRecording ? '#d1d5db' : '#ef4444', color: '#ffffff', border: 'none', borderRadius: '0.375rem', cursor: !isRecording ? 'not-allowed' : 'pointer', flex: 1, minWidth: '120px' }}>
            ⏹ Stop Recording
          </button>
          <button onClick={handleCopyTranscript} disabled={!transcript} style={{ padding: '0.625rem 1rem', backgroundColor: !transcript ? '#d1d5db' : '#10b981', color: '#ffffff', border: 'none', borderRadius: '0.375rem', cursor: !transcript ? 'not-allowed' : 'pointer', flex: 1, minWidth: '120px' }}>
            {copyStatus ? copyStatus : '📋 Copy'}
          </button>
        </div>

        <div style={{ marginBottom: '1.5rem', padding: '0.75rem', backgroundColor: '#1f2937', borderRadius: '0.375rem', fontSize: '0.7rem', fontFamily: 'monospace', color: '#fbbf24', maxHeight: '80px', overflowY: 'auto' }}>
          <div style={{ marginBottom: '0.5rem', fontWeight: 'bold' }}>WEBSOCKET LOG</div>
          {wsLogs.map((log, i) => <div key={i}>{log}</div>)}
        </div>

        <div style={{ marginBottom: '1.5rem', padding: '0.75rem', backgroundColor: '#1f2937', borderRadius: '0.375rem', fontSize: '0.7rem', fontFamily: 'monospace', color: '#10b981', maxHeight: '80px', overflowY: 'auto' }}>
          <div style={{ marginBottom: '0.5rem', fontWeight: 'bold' }}>AUDIO LOG</div>
          {debugLogs.map((log, i) => <div key={i}>{log}</div>)}
        </div>

        <div style={{ backgroundColor: '#ffffff', border: '1px solid #e5e7eb', borderRadius: '0.5rem' }}>
          <div style={{ padding: '0.75rem 1rem', backgroundColor: '#f3f4f6', borderBottom: '1px solid #e5e7eb', fontSize: '0.875rem', fontWeight: '500', color: '#6b7280' }}>Transcript</div>
          <div ref={transcriptRef} style={{ padding: '1rem', minHeight: '200px', maxHeight: '400px', overflowY: 'auto', fontSize: '0.95rem', lineHeight: '1.6', color: '#111827', whiteSpace: 'pre-wrap', wordBreak: 'break-word', userSelect: 'text' }}>
            {transcript || <span style={{ color: '#9ca3af', fontStyle: 'italic' }}>Start recording to see transcript appear here...</span>}
          </div>
        </div>
      </div>

      <style>{`@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }`}</style>
    </div>
  );
}