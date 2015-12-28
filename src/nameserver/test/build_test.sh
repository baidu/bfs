
# gtest
# wget --no-check-certificate https://googletest.googlecode.com/files/gtest-1.7.0.zip
git clone --depth=1 https://github.com/xupeilin/gtest_archive
mv gtest_archive/gtest-1.7.0.zip .
unzip gtest-1.7.0.zip

work_dir=`pwd`
mkdir -p gtest/lib
mkdir -p gtest/include

cd gtest-1.7.0
./configure --prefix=$work_dir/gtest --disable-shared --with-pic
make
cp -a lib/.libs/* $work_dir/gtest/lib
cp -a include/gtest $work_dir/gtest/include
cd -

rm -rf gtest_archive gtest-1.7.0.zip gtest-1.7.0

make
