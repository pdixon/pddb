// Copyright (C) 2014 Phillip Dixon
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include "pddb.hpp"

#include <cassert>
#include <iostream>
#include <memory>
#include <tuple>

using namespace pddb;

int main()
{
    auto db = std::unique_ptr<database>{new database()};
    auto stmt = db->prepare("CREATE TABLE test (id PRIMARY KEY, name STRING, d FLOAT);");
    assert(stmt->step() == result::DONE);
    assert(db->execute("INSERT INTO test VALUES (1, 'Hello World', 2.5)") == result::DONE);
    assert(db->execute("INSERT INTO test VALUES (4, 'Just a Test', 4.75)") == result::DONE);
    auto query = db->prepare("SELECT * FROM test");
    for(const auto &r: query->data<int, std::string, double>()) {
        std::cout << std::get<0>(r) << ", "
                  << std::get<1>(r) << ", "
                  << std::get<2>(r) << std::endl;
    }
    stmt = db->prepare("INSERT INTO test VALUES (?, ?, ?)");
    stmt->bind(3, 1);
    stmt->bind("And we can bind", 2);
    stmt->bind(3.14, 3);
    stmt->step();
    for(const auto &r: query->data<int, std::string, double>()) {
        std::cout << std::get<0>(r) << " "
                  << std::get<1>(r) << " "
                  << std::get<2>(r) << std::endl;
    }
}
