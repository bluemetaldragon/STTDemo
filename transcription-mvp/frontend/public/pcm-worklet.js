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
      const msg = event.data;
      if (msg && msg.type === 'stop') {
        this.flush();
        this.isStopped = true;
      }
    };

    this.port.postMessage({
      type: 'log',
      message: `Worklet initialized (input=${this.inputSampleRate}Hz, target=${this.targetSampleRate}Hz, chunk=${this.chunkDurationMs}ms)`,
    });
  }

  process(inputs) {
    if (this.isStopped) {
      return false;
    }

    const input = inputs[0];
    if (!input || !input[0] || input[0].length === 0) {
      return true;
    }

    const monoChannel = input[0];

    for (let i = 0; i < monoChannel.length; i += 1) {
      this.sourceBuffer.push(monoChannel[i]);
    }

    this.emitAvailableChunks();
    return true;
  }

  emitAvailableChunks() {
    const neededInputSamples = Math.ceil(
      this.chunkSamples * (this.inputSampleRate / this.targetSampleRate)
    );

    while (this.sourceBuffer.length >= neededInputSamples) {
      const inputChunk = this.sourceBuffer.slice(0, neededInputSamples);
      this.sourceBuffer = this.sourceBuffer.slice(neededInputSamples);

      const downsampled = this.downsampleBuffer(inputChunk, this.inputSampleRate, this.targetSampleRate);
      const pcm16 = this.floatTo16BitPCM(downsampled);

      this.port.postMessage(
        {
          type: 'pcm',
          buffer: pcm16.buffer,
          byteLength: pcm16.byteLength,
          samples: pcm16.length,
          sampleRate: this.targetSampleRate,
        },
        [pcm16.buffer]
      );
    }
  }

  flush() {
    if (!this.sourceBuffer.length) {
      return;
    }

    const downsampled = this.downsampleBuffer(
      this.sourceBuffer,
      this.inputSampleRate,
      this.targetSampleRate
    );

    if (!downsampled.length) {
      this.sourceBuffer = [];
      return;
    }

    const pcm16 = this.floatTo16BitPCM(downsampled);

    this.port.postMessage(
      {
        type: 'pcm',
        buffer: pcm16.buffer,
        byteLength: pcm16.byteLength,
        samples: pcm16.length,
        sampleRate: this.targetSampleRate,
      },
      [pcm16.buffer]
    );

    this.sourceBuffer = [];
  }

  downsampleBuffer(buffer, inputRate, outputRate) {
    if (outputRate === inputRate) {
      return new Float32Array(buffer);
    }

    if (outputRate > inputRate) {
      throw new Error('Output sample rate must be less than or equal to input sample rate');
    }

    const sampleRateRatio = inputRate / outputRate;
    const newLength = Math.round(buffer.length / sampleRateRatio);
    const result = new Float32Array(newLength);

    let offsetResult = 0;
    let offsetBuffer = 0;

    while (offsetResult < result.length) {
      const nextOffsetBuffer = Math.round((offsetResult + 1) * sampleRateRatio);
      let accum = 0;
      let count = 0;

      for (let i = offsetBuffer; i < nextOffsetBuffer && i < buffer.length; i += 1) {
        accum += buffer[i];
        count += 1;
      }

      result[offsetResult] = count > 0 ? accum / count : 0;
      offsetResult += 1;
      offsetBuffer = nextOffsetBuffer;
    }

    return result;
  }

  floatTo16BitPCM(float32Array) {
    const pcm16 = new Int16Array(float32Array.length);

    for (let i = 0; i < float32Array.length; i += 1) {
      let s = Math.max(-1, Math.min(1, float32Array[i]));
      pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
    }

    return pcm16;
  }
}

registerProcessor('pcm-worklet', PCMWorkletProcessor);