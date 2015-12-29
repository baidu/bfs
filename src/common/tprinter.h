// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#ifndef  BAIDU_COMMON_TPRINTER_H_
#define  BAIDU_COMMON_TPRINTER_H_

#include <stdint.h>

#include <string>
#include <vector>

using std::string;

namespace common {

class TPrinter {
public:
    typedef std::vector<string> Line;
    typedef std::vector<Line> Table;

    TPrinter();
    TPrinter(int cols);
    ~TPrinter();

    bool AddRow(const std::vector<string>& cols);

    bool AddRow(int argc, ...);

    bool AddRow(const std::vector<int64_t>& cols);

    void Print(bool has_head = true);

    string ToString(bool has_head = true);

    void Reset();

    void Reset(int cols);

    static string RemoveSubString(const string& input, const string& substr);

private:
    static const uint32_t kMaxColWidth = 50;
    size_t _cols;
    std::vector<int> _col_width;
    Table _table;
};

} // namespace common
#endif // BAIDU_COMMON_TPRINTER_H_
