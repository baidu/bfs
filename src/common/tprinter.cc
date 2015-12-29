// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#include "tprinter.h"

#include <stdarg.h>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "string_util.h"

namespace common {

TPrinter::TPrinter() : _cols(0) {
}

TPrinter::TPrinter(int cols) : _cols(cols) {
    if (cols > 0) {
        _col_width.resize(cols, 0);
    }
}

TPrinter::~TPrinter() {}

bool TPrinter::AddRow(const std::vector<string>& cols) {
    if (cols.size() != _cols) {
        std::cerr << "arg num error: " << cols.size() << " vs " << _cols << std::endl;
        return false;
    }
    Line line;
    for (size_t i = 0; i < cols.size(); ++i) {
        string item = cols[i];
        if (item.size() > kMaxColWidth) {
            item = item.substr(0, kMaxColWidth);
        }
        if (item.size() > static_cast<uint32_t>(_col_width[i])) {
            _col_width[i] = item.size();
        }
        if (item.size() == 0) {
            item = "-";
        }
        line.push_back(item);
    }
    _table.push_back(line);
    return true;
}

bool TPrinter::AddRow(int argc, ...) {
    if (static_cast<uint32_t>(argc) != _cols) {
        std::cerr << "arg num error: " << argc << " vs " << _cols << std::endl;
        return false;
    }
    std::vector<string> v;
    va_list args;
    va_start(args, argc);
    for (int i = 0; i < argc; ++i) {
        string item = va_arg(args, char*);
        v.push_back(item);
    }
    va_end(args);
    return AddRow(v);
}

bool TPrinter::AddRow(const std::vector<int64_t>& cols) {
    if (cols.size() != _cols) {
        std::cerr << "arg num error: " << cols.size() << " vs " << _cols << std::endl;
        return false;
    }
    std::vector<string> v;
    for (size_t i = 0; i < cols.size(); ++i) {
        v.push_back(common::NumToString(cols[i]));
    }
    return AddRow(v);
}

void TPrinter::Print(bool has_head) {
    if (_table.size() < 1) {
        return;
    }
    int line_len = 0;
    for (size_t i = 0; i < _cols; ++i) {
        line_len += 2 + _col_width[i];
        std::cout << "  " << std::setfill(' ')
            << std::setw(_col_width[i])
            << std::setiosflags(std::ios::left)
            << _table[0][i];
    }
    std::cout << std::endl;
    if (has_head) {
        for (int i = 0; i < line_len + 2; ++i) {
            std::cout << "-";
        }
        std::cout << std::endl;
    }

    for (size_t i = 1; i < _table.size(); ++i) {
        for (size_t j = 0; j < _cols; ++j) {
            std::cout << "  " << std::setfill(' ')
                << std::setw(_col_width[j])
                << std::setiosflags(std::ios::left)
                << _table[i][j];
        }
        std::cout << std::endl;
    }
}

string TPrinter::ToString(bool has_head) {
    std::ostringstream ostr;
    if (_table.size() < 1) {
        return "";
    }
    int line_len = 0;
    for (size_t i = 0; i < _cols; ++i) {
        line_len += 2 + _col_width[i];
        ostr << "  " << std::setfill(' ')
            << std::setw(_col_width[i])
            << std::setiosflags(std::ios::left)
            << _table[0][i];
    }
    ostr << std::endl;
    if (has_head) {
        for (int i = 0; i < line_len + 2; ++i) {
            ostr << "-";
        }
        ostr << std::endl;
    }

    for (size_t i = 1; i < _table.size(); ++i) {
        for (size_t j = 0; j < _cols; ++j) {
            ostr << "  " << std::setfill(' ')
                << std::setw(_col_width[j])
                << std::setiosflags(std::ios::left)
                << _table[i][j];
        }
        ostr << std::endl;
    }
    return ostr.str();
}

void TPrinter::Reset() {
    std::vector<int> tmp(_cols, 0);
    _col_width.swap(tmp);
    _table.clear();
}

void TPrinter::Reset(int cols) {
    _cols = cols;
    Reset();
}

string TPrinter::RemoveSubString(const string& input, const string& substr) {
    string ret;
    string::size_type p = 0;
    string tmp = input;
    while (1) {
        tmp = tmp.substr(p);
        p = tmp.find(substr);
        ret.append(tmp.substr(0, p));
        if (p == string::npos) {
            break;
        }
        p += substr.size();
    }

    return ret;
}

} // namespace common

