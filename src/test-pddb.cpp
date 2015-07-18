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
    return [](Types... args)
    {
        return T(args...);
    };
}

template <typename T>
auto construct = [](auto &&... args)
{
    return T(args...);
};

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
    auto db = std::make_unique<database>();
    db->execute("CREATE TABLE test (id PRIMARY KEY, name STRING, d FLOAT);");

    auto stmt = db->prepare_stmt("INSERT INTO test VALUES (?, ?, ?)");
    *stmt = std::make_tuple(1, "Hello World", 2.5);
    *stmt = std::make_tuple(2, "Just a Test", 4.75);
    auto query = db->prepare<int, std::string, double>("SELECT * FROM test");
    for(const auto &r : *query)
    {
        std::cout << std::get<0>(r) << ", " << std::get<1>(r) << ", "
                  << std::get<2>(r) << std::endl;
    }
    *stmt = std::make_tuple(3, "And we can bind", 3.14);

    for(const auto &r : *query)
    {
        std::cout << std::get<0>(r) << " " << std::get<1>(r) << " "
                  << std::get<2>(r) << std::endl;
    }
    try
    {
        *stmt = std::make_tuple(4, "incomplete");
    }
    catch(const error &e)
    {
        std::cout << e.what() << std::endl;
    }

    std::cout << "\nTransaction Rollback" << std::endl;
    {
        auto t = db->start_transaction();
        db->execute("INSERT INTO test VALUES (5, 'Dont see this', 4.75)");
    }
    query = db->prepare<int, std::string, double>("SELECT * FROM test");
    for(const auto &r : *query)
    {
        std::cout << std::get<0>(r) << ", " << std::get<1>(r) << ", "
                  << std::get<2>(r) << std::endl;
    }

    std::cout << "\nTransaction Commit" << std::endl;
    {
        auto t = db->start_transaction();
        db->execute("INSERT INTO test VALUES (6, 'Do see this', 4.75)");
        t->commit();
    }
    for(const auto &r : *query)
    {
        std::cout << std::get<0>(r) << ", " << std::get<1>(r) << ", "
                  << std::get<2>(r) << std::endl;
    }

    std::cout << "\nApply result" << std::endl;
    auto v = std::vector<bar>();
    for(const auto &r : *query)
    {
        v.push_back(apply(make_bar, r));
    }
    std::copy(begin(v), end(v), std::ostream_iterator<bar>(std::cout, "\n"));

    std::cout << "\nSTL FTW" << std::endl;
    std::transform(begin(*query), end(*query),
                   std::ostream_iterator<bar>(std::cout, "\n"),
                   [](const auto &r)
                   {
                       return build<bar>(r);
                   });

    std::cout << "\nSTL FTW" << std::endl;
    std::transform(begin(*query), end(*query),
                   std::ostream_iterator<bar>(std::cout, "\n"),
                   build<bar, std::tuple<int, std::string, double>>);

    std::cout << "\nSTL FTW" << std::endl;
    auto con = construct<bar>;
    auto bound = [&con](const auto &r)
    {
        return apply(con, r);
    };

    std::transform(begin(*query), end(*query),
                   std::ostream_iterator<bar>(std::cout, "\n"), bound);

    std::cout << "\nNULL Query" << std::endl;
    db->execute("INSERT INTO test VALUES (7, NULL, NULL)");
    auto optional_query =
        db->prepare<int, optional<std::string>, optional<double>>(
            "SELECT * FROM test");
    for(const auto &r : *optional_query)
    {
        std::cout << std::get<0>(r) << ", "
                  << std::get<1>(r).value_or("My NULL") << ", "
                  << std::get<2>(r).value_or(0.0) << std::endl;
    }
    for(const auto &r : *query)
    {
        std::cout << std::get<0>(r) << ", " << std::get<1>(r) << ", "
                  << std::get<2>(r) << std::endl;
    }
}
