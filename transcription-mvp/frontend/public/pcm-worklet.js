// PCM Audio Worklet Processor
// Receives Float32 audio, resamples to 16 kHz, converts to 16-bit PCM,
// and posts messages back to the main thread.

class PCMWorkletProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.sampleRate = sampleRate;          // native sample rate (e.g. 48000)
    this.targetRate = 16000;
    this.buffer = new Float32Array(0);      // accumulated resampled data (float)
    this.chunkInterval = 0.25;              // seconds
    this.lastSentTime = currentTime;

    this.port.onmessage = (event) => {
      if (event.data === 'stop') {
        this.stop = true;
      }
    };
  }

  // Linear interpolation resampler
  resample(input, fromRate, toRate) {
    const ratio = fromRate / toRate;
    const newLength = Math.round(input.length / ratio);
    const output = new Float32Array(newLength);
    for (let i = 0; i < newLength; i++) {
      const srcIndex = i * ratio;
      const low = Math.floor(srcIndex);
      const high = Math.min(low + 1, input.length - 1);
      const frac = srcIndex - low;
      output[i] = input[low] * (1 - frac) + input[high] * frac;
    }
    return output;
  }

  // Float32 (-1..1) -> Int16 PCM
  float32ToInt16(float32Array) {
    const int16Array = new Int16Array(float32Array.length);
    for (let i = 0; i < float32Array.length; i++) {
      const s = Math.max(-1, Math.min(1, float32Array[i]));
      int16Array[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
    }
    return int16Array;
  }

  process(inputs, outputs, parameters) {
    if (this.stop) return false; // stop processing

    const input = inputs[0];
    if (!input || input.length === 0) return true;

    const channelData = input[0]; // Float32Array, mono
    if (!channelData) return true;

    // Resample to 16 kHz
    const resampled = this.resample(channelData, this.sampleRate, this.targetRate);
    // Append to buffer
    const newBuffer = new Float32Array(this.buffer.length + resampled.length);
    newBuffer.set(this.buffer);
    newBuffer.set(resampled, this.buffer.length);
    this.buffer = newBuffer;

    // Send chunk every chunkInterval seconds (approx)
    const now = currentTime;
    if (now - this.lastSentTime >= this.chunkInterval && this.buffer.length > 0) {
      // Convert to Int16
      const pcm16 = this.float32ToInt16(this.buffer);
      // Post back to main thread as ArrayBuffer
      this.port.postMessage(pcm16.buffer, [pcm16.buffer]); // transfer ownership
      this.buffer = new Float32Array(0);
      this.lastSentTime = now;
    }

    return true;
  }
}

registerProcessor('pcm-worklet', PCMWorkletProcessor);