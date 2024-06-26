require "./lib"

# A read-only `IO` object to decompress data in the LZ4 frame format.
#
# Instances of this class wrap another IO object. When you read from this instance
# instance, it reads data from the underlying IO, decompresses it, and returns
# it to the caller.
# ## Example: decompress an lz4 file
# ```
# require "lz4"

# string = File.open("file.lz4") do |file|
#    Compress::LZ4::Reader.open(file) do |lz4|
#      lz4.gets_to_end
#    end
# end
# pp string
# ```
class Compress::LZ4::Reader < ::IO
  property? sync_close : Bool
  getter? closed = false
  getter compressed_bytes = 0u64
  getter uncompressed_bytes = 0u64
  @context : LibLZ4::Dctx
  @opts = LibLZ4::DecompressOptionsT.new(stable_dst: 0)

  def initialize(@io : ::IO, @sync_close = false)
    ret = LibLZ4.create_decompression_context(out @context, LibLZ4::VERSION)
    raise_if_error(ret, "Failed to create decompression context")
    @buffer = Bytes.new(64 * 1024)
    @buffer_rem = Bytes.empty
  end

  # Creates a new reader from the given *io*, yields it to the given block,
  # and closes it at its end.
  def self.open(io : ::IO, sync_close : Bool = false)
    reader = new(io, sync_close: sync_close)
    yield reader ensure reader.close
  end

  # Creates a new reader from the given *filename*.
  def self.new(filename : String)
    new(::File.new(filename), sync_close: true)
  end

  # Creates a new reader from the given *io*, yields it to the given block,
  # and closes it at the end.
  def self.open(io : ::IO, sync_close = false)
    reader = new(io, sync_close: sync_close)
    yield reader ensure reader.close
  end

  # Creates a new reader from the given *filename*, yields it to the given block,
  # and closes it at the end.
  def self.open(filename : String)
    reader = new(filename)
    yield reader ensure reader.close
  end

  # Always raises `IO::Error` because this is a read-only `IO`.
  def write(slice : Bytes) : Nil
    raise IO::Error.new "Can't write to LZ4::Reader"
  end

  def read(slice : Bytes) : Int32
    check_open
    return 0 if slice.empty?
    decompressed_bytes = 0
    hint = 0u64 # the hint from the last decompression
    loop do
      src_remaining = @buffer_rem.size.to_u64
      src_remaining = Math.min(hint, src_remaining) unless hint.zero?
      dst_remaining = slice.size.to_u64

      hint = LibLZ4.decompress(@context, slice, pointerof(dst_remaining), @buffer_rem, pointerof(src_remaining), pointerof(@opts))
      raise_if_error(hint, "Failed to decompress")

      @buffer_rem += src_remaining
      slice += dst_remaining
      decompressed_bytes += dst_remaining
      break if slice.empty? # got all we needed
      break if hint.zero?   # hint of how much more src data is needed
      refill_buffer
      break if @buffer_rem.empty?
    end
    @uncompressed_bytes &+= decompressed_bytes
    decompressed_bytes
  end

  def flush
    raise IO::Error.new "Can't flush LZ4::Reader"
  end

  def close
    if @sync_close
      @io.close
      @closed = true # Only really closed if io is closed
    end
  end

  def finalize
    LibLZ4.free_decompression_context(@context)
  end

  def rewind
    @io.rewind
    @buffer_rem = Bytes.empty
    @uncompressed_bytes = 0u64
    @compressed_bytes = 0u64
    LibLZ4.reset_decompression_context(@context)
  end

  private def refill_buffer
    return unless @buffer_rem.empty? # never overwrite existing buffer
    cnt = @io.read(@buffer)
    @compressed_bytes &+= cnt
    @buffer_rem = @buffer[0, cnt]
  end

  private def raise_if_error(ret : Int, msg : String)
    if LibLZ4.is_error(ret) != 0
      raise LZ4Error.new("#{msg}: #{String.new(LibLZ4.get_error_name(ret))}")
    end
  end

  # Uncompressed bytes outputted / compressed bytes read so far in the stream
  def compression_ratio : Float64
    return 0.0 if @compressed_bytes.zero?
    @uncompressed_bytes / @compressed_bytes
  end
end
