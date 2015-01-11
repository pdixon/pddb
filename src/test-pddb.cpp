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

#include <algorithm>
#include <cassert>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

using namespace pddb;

template <class F, class Tuple, std::size_t... I>
auto apply_impl(F &&f, Tuple &&t, std::index_sequence<I...>)
{
    return std::forward<F>(f)(std::get<I>(std::forward<Tuple>(t))...);
}

template <class F, class Tuple>
auto apply(F &&f, Tuple &&t)
{
    using Indices =
        std::make_index_sequence<std::tuple_size<std::decay_t<Tuple>>::value>;
    return apply_impl(std::forward<F>(f), std::forward<Tuple>(t), Indices());
}

template <class F, class Tuple, std::size_t... I>
auto build_impl(Tuple &&t, std::index_sequence<I...>)
{
    return F(std::get<I>(std::forward<Tuple>(t))...);
}

template <class F, class Tuple>
auto build(Tuple &&t)
{
    using Indices =
        std::make_index_sequence<std::tuple_size<std::decay_t<Tuple>>::value>;
    return build_impl<F>(std::forward<Tuple>(t), Indices());
}

template <typename T, class... Types>
auto constructor()
{
    return [](Types... args){ return T(args...); };
}

template <typename T>
auto construct = [](auto&&... args){ return T(args...); };

void foo(int a, const std::string &b, double c)
{
    std::cout << a << ", " << b << ", " << c << std::endl;
}

class bar
{
    int a;
    std::string b;
    double c;

  public:
    bar(int a, std::string b, double c) : a(a), b(b), c(c){};
    friend std::ostream &operator<<(std::ostream &os, bar b);
};

std::ostream &operator<<(std::ostream &os, bar b)
{
    os << b.a << ", " << b.b << ", " << b.c;
    return os;
}

auto make_bar(int a, const std::string &b, double c) { return bar(a, b, c); }


int main()
{
    auto db = std::unique_ptr<database>{new database()};
    auto stmt = db->prepare(
        "CREATE TABLE test (id PRIMARY KEY, name STRING, d FLOAT);");
    assert(stmt->step() == result::DONE);
    assert(db->execute("INSERT INTO test VALUES (1, 'Hello World', 2.5)") ==
           result::DONE);
    assert(db->execute("INSERT INTO test VALUES (4, 'Just a Test', 4.75)") ==
           result::DONE);
    auto query = db->prepare("SELECT * FROM test");
    for(const auto &r : query->data<int, std::string, double>())
    {
        std::cout << std::get<0>(r) << ", " << std::get<1>(r) << ", "
                  << std::get<2>(r) << std::endl;
    }
    stmt = db->prepare("INSERT INTO test VALUES (?, ?, ?)");
    stmt->bind(3, 1);
    stmt->bind("And we can bind", 2);
    stmt->bind(3.14, 3);
    stmt->step();
    for(const auto &r : query->data<int, std::string, double>())
    {
        std::cout << std::get<0>(r) << " " << std::get<1>(r) << " "
                  << std::get<2>(r) << std::endl;
    }
    try
    {
        stmt->bind(4, 1);
        stmt->bind("incomplete", 2);
        stmt->step();
    }
    catch(const error &e)
    {
        std::cout << e.what() << std::endl;
    }

    std::cout << "\nTransaction Rollback" << std::endl;
    {
        auto t = db->transaction();
        assert(
            db->execute("INSERT INTO test VALUES (5, 'Dont see this', 4.75)") ==
            result::DONE);
    }
    query = db->prepare("SELECT * FROM test");
    for(const auto &r : query->data<int, std::string, double>())
    {
        std::cout << std::get<0>(r) << ", " << std::get<1>(r) << ", "
                  << std::get<2>(r) << std::endl;
    }

    std::cout << "\nTransaction Commit" << std::endl;
    {
        auto t = db->transaction();
        assert(
            db->execute("INSERT INTO test VALUES (6, 'Do see this', 4.75)") ==
            result::DONE);
        t->commit();
    }
    for(const auto &r : query->data<int, std::string, double>())
    {
        std::cout << std::get<0>(r) << ", " << std::get<1>(r) << ", "
                  << std::get<2>(r) << std::endl;
    }

    std::cout << "\nApply result" << std::endl;
    auto v = std::vector<bar>();
    for(const auto &r : query->data<int, std::string, double>())
    {
        v.push_back(apply(make_bar, r));
    }
    std::copy(begin(v), end(v), std::ostream_iterator<bar>(std::cout, "\n"));

    std::cout << "\nSTL FTW" << std::endl;
    auto d = query->data<int, std::string, double>();
    std::transform(begin(d), end(d),
                   std::ostream_iterator<bar>(std::cout, "\n"),
                   [](const auto &r)
                   {
        return build<bar>(r);
    });

    std::cout << "\nSTL FTW" << std::endl;
    d = query->data<int, std::string, double>();
    std::transform(begin(d), end(d),
                   std::ostream_iterator<bar>(std::cout, "\n"),
                   build<bar, std::tuple<int, std::string, double>>
                   );


    std::cout << "\nSTL FTW" << std::endl;
    d = query->data<int, std::string, double>();
    auto con = construct<bar>;
    auto bound = [&con](const auto& r){ return apply(con, r); };

    std::transform(begin(d), end(d),
                   std::ostream_iterator<bar>(std::cout, "\n"),
                   bound
                   );

}
