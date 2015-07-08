require 'formula'

class Tarantool < Formula
  homepage 'http://tarantool.org'
  url 'http://tarantool.org/dist/master/tarantool-1.6.5-99-gac674ae-src.tar.gz '
  md5 '7511325d5bc1a42a3f781ff5fc9396b3' 
  head 'https://github.com/mailru/tarantool.git', :using => :git

  depends_on 'cmake' => :build

  def install
    system "cmake", ".",
           "-DCMAKE_BUILD_TYPE=RelWithDebugInfo",
           "-DCMAKE_LOCALSTATE_DIR=#{prefix}/var",
           "-DCMAKE_SYSCONF_DIR=#{prefix}/etc",
           *std_cmake_args
    system "make"
    system "make install"
  end
end

