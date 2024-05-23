require "./lib"
require "socket"

class Compress::LZ4::IO < ::IO
  property? sync_close : Bool
  getter? closed = false
  property! remote_address : Socket::Address
  property! local_address : Socket::Address
  getter uncompressed_bytes_in = 0u64
  getter compressed_bytes_in = 0u64
  getter uncompressed_bytes_out = 0u64
  getter compressed_bytes_out = 0u64

  @dctx : LibLZ4::Dctx
  @dopts = LibLZ4::DecompressOptionsT.new(stable_dst: 0)

  @cctx : LibLZ4::Cctx
  @copts = LibLZ4::CompressOptionsT.new(stable_src: 0)
  @pref : LibLZ4::PreferencesT
  @header_written = false
  private MaxSrcSize = 64 * 1024

  def initialize(@io : ::IO, compress_options = CompressOptions.new, @sync_close = false)
    ret = LibLZ4.create_decompression_context(out @dctx, LibLZ4::VERSION)
    raise_if_error(ret, "Failed to create decompression context")
    @read_buffer = Bytes.new(64 * 1024)
    @read_buffer_rem = Bytes.empty

    ret = LibLZ4.create_compression_context(out @cctx, LibLZ4::VERSION)
    raise_if_error(ret, "Failed to create compression context")
    @pref = compress_options.to_preferences
    buffer_size = LibLZ4.compress_bound(MaxSrcSize, pointerof(@pref))
    @write_buffer = Bytes.new(buffer_size)
  end

  def read(slice : Bytes) : Int32
    check_open
    return 0 if slice.empty?
    decompressed_bytes = 0
    hint = 0u64 # the hint from the last decompression
    loop do
      src_remaining = @read_buffer_rem.size.to_u64
      src_remaining = Math.min(hint, src_remaining) unless hint.zero?
      dst_remaining = slice.size.to_u64

      hint = LibLZ4.decompress(@dctx, slice, pointerof(dst_remaining), @read_buffer_rem, pointerof(src_remaining), pointerof(@dopts))
      raise_if_error(hint, "Failed to decompress")

      @read_buffer_rem += src_remaining
      slice += dst_remaining
      decompressed_bytes += dst_remaining
      break if slice.empty? # got all we needed
      break if hint.zero?   # hint of how much more src data is needed
      refill_buffer
      break if @read_buffer_rem.empty?
    end
    @uncompressed_bytes_in &+= decompressed_bytes
    decompressed_bytes
  end

  # Ends the current LZ4 frame, the stream can still be written to, unless @sync_close
  def close
    check_open
    ret = LibLZ4.compress_end(@cctx, @write_buffer, @write_buffer.size, pointerof(@copts))
    raise_if_error(ret, "Failed to end frame")
    @compressed_bytes_out &+= ret
    @io.write(@write_buffer[0, ret])
    @io.flush
    @header_written = false
  ensure
    if @sync_close
      @closed = true # the stream can still be written until the underlaying io is closed
      @io.close
    end
  end

  def finalize
    LibLZ4.free_decompression_context(@dctx)
    LibLZ4.free_compression_context(@cctx)
  end

  def rewind
    check_open
    if @header_written
      ret = LibLZ4.compress_end(@cctx, @write_buffer, @write_buffer.size, pointerof(@copts))
      raise_if_error(ret, "Failed to end frame")
      @io.write(@write_buffer[0, ret])
      @io.flush
      @header_written = false
    end
    @compressed_bytes_out = @uncompressed_bytes_out = @compressed_bytes_in = @uncompressed_bytes_in = 0u64
    @read_buffer_rem = Bytes.empty
    LibLZ4.reset_decompression_context(@dctx)
    @io.rewind
  end

  private def refill_buffer
    return unless @read_buffer_rem.empty? # never overwrite existing buffer
    cnt = @io.read(@read_buffer)
    @compressed_bytes_in &+= cnt
    @read_buffer_rem = @read_buffer[0, cnt]
  end

  private def raise_if_error(ret : Int, msg : String)
    if LibLZ4.is_error(ret) != 0
      raise LZ4Error.new("#{msg}: #{String.new(LibLZ4.get_error_name(ret))}")
    end
  end

  def write(slice : Bytes) : Nil
    check_open
    write_header
    @uncompressed_bytes_out &+= slice.size
    until slice.empty?
      read_size = Math.min(slice.size, MaxSrcSize)
      @copts.stable_src = slice.size > MaxSrcSize ? 1 : 0
      ret = LibLZ4.compress_update(@cctx, @write_buffer, @write_buffer.size, slice, read_size, pointerof(@copts))
      raise_if_error(ret, "Failed to compress")
      @compressed_bytes_out &+= ret
      @io.write(@write_buffer[0, ret])
      slice += read_size
    end
  end

  private def write_header
    return if @header_written
    ret = LibLZ4.compress_begin(@cctx, @write_buffer, @write_buffer.size, pointerof(@pref))
    raise_if_error(ret, "Failed to begin compression")
    @io.write(@write_buffer[0, ret])
    @compressed_bytes_out &+= ret
    @header_written = true
  end

  # Flush LZ4 lib buffers even if a block isn't full
  def flush : Nil
    check_open
    ret = LibLZ4.flush(@cctx, @write_buffer, @write_buffer.size, pointerof(@copts))
    raise_if_error(ret, "Failed to flush")
    @compressed_bytes_out &+= ret
    @io.write(@write_buffer[0, ret])
    @io.flush
  end
end
