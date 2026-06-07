class PCMWorkletProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.inputSampleRate = sampleRate;
    this.targetSampleRate = 16000;
    this.chunkDurationMs = 250;
    this.chunkSamples = Math.floor(this.targetSampleRate * (this.chunkDurationMs / 1000));

    this.sourceBuffer = [];
    this.isStopped = false;

    this.port.onmessage = (event) => {
      if (event.data && event.data.type === 'stop') {
        this.flush();
        this.isStopped = true;
      }
    };
  }

  process(inputs) {
    if (this.isStopped) return false;

    const input = inputs[0];
    if (!input || !input[0]) return true;

    const monoChannel = input[0];
    for (let i = 0; i < monoChannel.length; i++) {
      this.sourceBuffer.push(monoChannel[i]);
    }

    this.emitAvailableChunks();
    return true;
  }

  emitAvailableChunks() {
    const ratio = this.inputSampleRate / this.targetSampleRate;
    const neededInputSamples = Math.ceil(this.chunkSamples * ratio);

    while (this.sourceBuffer.length >= neededInputSamples) {
      const inputChunk = this.sourceBuffer.slice(0, neededInputSamples);
      this.sourceBuffer = this.sourceBuffer.slice(neededInputSamples);

      const downsampled = this.downsample(inputChunk, ratio);
      const pcm16 = this.floatTo16BitPCM(downsampled);

      this.port.postMessage({
        type: 'pcm',
        buffer: pcm16.buffer // Transferable
      }, [pcm16.buffer]);
    }
  }

  flush() {
    if (this.sourceBuffer.length === 0) return;
    const ratio = this.inputSampleRate / this.targetSampleRate;
    const downsampled = this.downsample(this.sourceBuffer, ratio);
    const pcm16 = this.floatTo16BitPCM(downsampled);
    this.port.postMessage({ type: 'pcm', buffer: pcm16.buffer }, [pcm16.buffer]);
    this.sourceBuffer = [];
  }

  downsample(buffer, ratio) {
    const newLength = Math.round(buffer.length / ratio);
    const result = new Float32Array(newLength);
    for (let i = 0; i < newLength; i++) {
      result[i] = buffer[Math.round(i * ratio)];
    }
    return result;
  }

  floatTo16BitPCM(float32Array) {
    const pcm16 = new Int16Array(float32Array.length);
    for (let i = 0; i < float32Array.length; i++) {
      let s = Math.max(-1, Math.min(1, float32Array[i]));
      pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
    }
    return pcm16;
  }
}

registerProcessor('pcm-worklet', PCMWorkletProcessor);