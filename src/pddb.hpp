// Copyright (C) 2014-15 Phillip Dixon
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

#pragma once

#include <experimental/optional>
#include <cassert>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#define SQLITE_HAS_CODEC
#include "sqlite3.h"

using std::experimental::optional;
using std::experimental::make_optional;

namespace pddb
{

namespace detail
{
template <class F, class... Args>
inline auto INVOKE(F &&f, Args &&... args)
    -> decltype(std::forward<F>(f)(std::forward<Args>(args)...))
{
    return std::forward<F>(f)(std::forward<Args>(args)...);
}

template <class Base, class T, class Derived>
inline auto INVOKE(T Base::*pmd, Derived &&ref)
    -> decltype(std::forward<Derived>(ref).*pmd)
{
    return std::forward<Derived>(ref).*pmd;
}

template <class PMD, class Pointer>
inline auto INVOKE(PMD pmd, Pointer &&ptr)
    -> decltype((*std::forward<Pointer>(ptr)).*pmd)
{
    return (*std::forward<Pointer>(ptr)).*pmd;
}

template <class Base, class T, class Derived, class... Args>
inline auto INVOKE(T Base::*pmf, Derived &&ref, Args &&... args)
    -> decltype((std::forward<Derived>(ref).*pmf)(std::forward<Args>(args)...))
{
    return (std::forward<Derived>(ref).*pmf)(std::forward<Args>(args)...);
}

template <class PMF, class Pointer, class... Args>
inline auto INVOKE(PMF pmf, Pointer &&ptr, Args &&... args)
    -> decltype(((*std::forward<Pointer>(ptr)).*
                 pmf)(std::forward<Args>(args)...))
{
    return ((*std::forward<Pointer>(ptr)).*pmf)(std::forward<Args>(args)...);
}
}

template <class F, class... ArgTypes>
decltype(auto) invoke(F &&f, ArgTypes &&... args)
{
    return detail::INVOKE(std::forward<F>(f), std::forward<ArgTypes>(args)...);
}

namespace detail
{
template <class F, class Tuple, std::size_t... I>
constexpr decltype(auto) apply_impl(F &&f, Tuple &&t, std::index_sequence<I...>)
{
    return invoke(std::forward<F>(f), std::get<I>(std::forward<Tuple>(t))...);
}

template <class F, class O, class Tuple, std::size_t... I>
constexpr decltype(auto) apply_impl(F &&f, O &&o, Tuple &&t,
                                    std::index_sequence<I...>)
{
    return invoke(std::forward<F>(f), o,
                  std::get<I>(std::forward<Tuple>(t))...);
}
}

template <class F, class Tuple>
constexpr decltype(auto) apply(F &&f, Tuple &&t)
{
    return detail::apply_impl(
        std::forward<F>(f), std::forward<Tuple>(t),
        std::make_index_sequence<std::tuple_size<std::decay_t<Tuple>>{}>{});
}

template <class F, class O, class Tuple>
constexpr decltype(auto) apply(F &&f, O &&o, Tuple &&t)
{
    return detail::apply_impl(
        std::forward<F>(f), std::forward<O>(o), std::forward<Tuple>(t),
        std::make_index_sequence<std::tuple_size<std::decay_t<Tuple>>{}>{});
}

template <class T>
struct remove_optional
{
    using type = T;
};

template <class T>
struct remove_optional<optional<T>>
{
    using type = T;
};

template <class T>
struct is_optional : std::false_type
{
};

template <class T>
struct is_optional<optional<T>> : std::true_type
{
};

enum class result
{
    OK = 0, /* Successful result */
    /* beginning-of-error-codes */
    ERROR = 1,       /* SQL error or missing database */
    INTERNAL = 2,    /* Internal logic error in SQLite */
    PERM = 3,        /* Access permission denied */
    ABORT = 4,       /* Callback routine requested an abort */
    BUSY = 5,        /* The database file is locked */
    LOCKED = 6,      /* A table in the database is locked */
    NOMEM = 7,       /* A malloc() failed */
    READONLY = 8,    /* Attempt to write a readonly database */
    INTERRUPT = 9,   /* Operation terminated by sqlite3_interrupt()*/
    IOERR = 10,      /* Some kind of disk I/O error occurred */
    CORRUPT = 11,    /* The database disk image is malformed */
    NOTFOUND = 12,   /* Unknown opcode in sqlite3_file_control() */
    FULL = 13,       /* Insertion failed because database is full */
    CANTOPEN = 14,   /* Unable to open the database file */
    PROTOCOL = 15,   /* Database lock protocol error */
    EMPTY = 16,      /* Database is empty */
    SCHEMA = 17,     /* The database schema changed */
    TOOBIG = 18,     /* String or BLOB exceeds size limit */
    CONSTRAINT = 19, /* Abort due to constraint violation */
    MISMATCH = 20,   /* Data type mismatch */
    MISUSE = 21,     /* Library used incorrectly */
    NOLFS = 22,      /* Uses OS features not supported on host */
    AUTH = 23,       /* Authorization denied */
    FORMAT = 24,     /* Auxiliary database format error */
    RANGE = 25,      /* 2nd parameter to sqlite3_bind out of range */
    NOTADB = 26,     /* File opened that is not a database file */
    NOTICE = 27,     /* Notifications from sqlite3_log() */
    WARNING = 28,    /* Warnings from sqlite3_log() */
    ROW = 100,       /* sqlite3_step() has another row ready */
    DONE = 101,      /* sqlite3_step() has finished executing */
};

class error : public std::runtime_error
{
  public:
    explicit error(const std::string &message) : std::runtime_error(message) {}
};

class database_internal
{
    friend class database;
    friend class statement_internal;
    template <typename... TS>
    friend class query;

  public:
    struct db
    {
        db() : db_(nullptr) {}
        db(sqlite3 *db) : db_(db) {}
        ~db() { sqlite3_close(db_); }
        operator sqlite3 *() { return db_; }
        sqlite3 *db_;
    };

    struct stmt
    {
        stmt(sqlite3_stmt *stmt) : stmt_(stmt) {}
        ~stmt() { sqlite3_finalize(stmt_); }
        result clear() { return result(sqlite3_clear_bindings(stmt_)); }
        result step() { return result(sqlite3_step(stmt_)); }
        result reset() { return result(sqlite3_reset(stmt_)); }
        size_t parameter_count() { return sqlite3_bind_parameter_count(stmt_); }
        result bind(int column, int value)
        {
            return result(sqlite3_bind_int(stmt_, column, value));
        }
        result bind(int column, int64_t value)
        {
            return result(sqlite3_bind_int64(stmt_, column, value));
        }
        result bind(int column, double value)
        {
            return result(sqlite3_bind_double(stmt_, column, value));
        }
        result bind(int column, std::string value)
        {
            return result(sqlite3_bind_text(stmt_, column, value.c_str(), -1,
                                            SQLITE_TRANSIENT));
        }
        result bind(int column, std::vector<uint8_t> value)
        {
            return result(sqlite3_bind_blob(stmt_, column, value.data(),
                                            value.size(), SQLITE_TRANSIENT));
        }
        optional<double> get_double(int column)
        {
            if(sqlite3_column_type(stmt_, column) == SQLITE_FLOAT)
            {
                return make_optional(sqlite3_column_double(stmt_, column));
            }
            else
            {
                return optional<double>();
            }
        }
        optional<int64_t> get_int(int column)
        {
            if(sqlite3_column_type(stmt_, column) == SQLITE_INTEGER)
            {
                return make_optional(sqlite3_column_int64(stmt_, column));
            }
            else
            {
                return optional<int64_t>();
            }
        }
        optional<std::string> get_string(int column)
        {
            if(sqlite3_column_type(stmt_, column) == SQLITE_TEXT)
            {
                const char *start = reinterpret_cast<const char *>(
                    sqlite3_column_text(stmt_, column));
                size_t length = sqlite3_column_bytes(stmt_, column);
                return make_optional(std::string(start, length));
            }
            else
            {
                return optional<std::string>();
            }
        }
        optional<std::vector<uint8_t>> get_blob(int column)
        {
            if(sqlite3_column_type(stmt_, column) == SQLITE_BLOB)
            {
                const uint8_t *start = reinterpret_cast<const uint8_t *>(
                    sqlite3_column_blob(stmt_, column));
                size_t length = sqlite3_column_bytes(stmt_, column);
                return make_optional(
                    std::vector<uint8_t>(start, start + length));
            }
            else
            {
                return optional<std::vector<uint8_t>>();
            }
        }

        operator sqlite3_stmt *() { return stmt_; }
        sqlite3_stmt *stmt_;
    };

    database_internal(const std::string &path, int flags)
    {
        sqlite3 *d;
        auto rc = result(sqlite3_open_v2(path.c_str(), &d, flags, nullptr));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(d));
        db_ = std::make_unique<db>(d);
    }

    database_internal(const std::string &path, const std::vector<uint8_t> &key,
                      int flags)
    {
        sqlite3 *d;
        auto rc = result(sqlite3_open_v2(path.c_str(), &d, flags, nullptr));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(d));
        rc = result(sqlite3_key(d, key.data(), key.size()));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(d));
        db_ = std::make_unique<db>(d);
    }

    void stmt_finalize(stmt *s)
    {
        statements.erase(std::find_if(begin(statements), end(statements),
                                      [=](const auto &i) -> bool
                                      {
                                          return i.get() == s;
                                      }));
    }

    stmt *prepare(const std::string &sql)
    {
        sqlite3_stmt *s;

        auto rc = result(sqlite3_prepare_v2(*db_, sql.data(), -1, &s, nullptr));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(*db_));
        auto i = statements.emplace(std::make_unique<stmt>(s));

        return i.first->get();
    }

    std::string err_msg(result r)
    {
        return sqlite3_errstr(static_cast<int>(r));
    }

  private:
    std::unordered_set<std::unique_ptr<stmt>> statements;
    std::unique_ptr<db> db_;
};

template <typename T>
T get_column_(database_internal::stmt *s, int current);

template <typename T>
T get_column_(database_internal::stmt *s, int current)
{
    using inner = typename remove_optional<T>::type;
    static_assert(std::is_integral<inner>::value, "");
    auto i = s->get_int(current);
    if(i)
    {
        return make_optional(static_cast<inner>(*i));
    }
    else
    {
        return optional<inner>();
    }
}

template <>
optional<double> get_column_(database_internal::stmt *s, int current)
{
    return s->get_double(current);
}

template <>
optional<std::string> get_column_(database_internal::stmt *s, int current)
{
    return s->get_string(current);
}

template <>
optional<std::vector<uint8_t>> get_column_(database_internal::stmt *s,
                                           int current)
{
    return s->get_blob(current);
}

class statement_internal
{
  public:
    statement_internal(const std::string &sql,
                       std::weak_ptr<database_internal> db)
        : db(db)
    {
        stmt = db.lock()->prepare(sql);
    }

    ~statement_internal()
    {
        auto locked = db.lock();
        if(locked)
        {
            locked->stmt_finalize(stmt);
        }
    }

    template <typename... Args>
    void bind(const Args &... args)
    {
        auto required = stmt->parameter_count();
        auto provided = sizeof...(args);

        if(required > provided)
        {
            throw error("Insufficient parameters provided");
        }
        if(required < provided)
        {
            throw error("Excess parameters provided");
        }
        auto locked = db.lock();
        if(locked)
        {
            auto rc = stmt->clear();
            if(rc != result::OK)
                throw error(locked->err_msg(rc));
            bind_(1, args...);
        }
        else
        {
            throw error("Database closed");
        }
    }

    template <typename... TS>
    std::tuple<TS...> row()
    {
        auto seq = std::make_index_sequence<sizeof...(TS)>{};
        return row_<std::tuple<TS...>>(seq);
    }

    bool step()
    {
        auto locked = db.lock();
        if(locked)
        {
            auto rc = stmt->step();
            if(rc < result::ROW)
                throw error(locked->err_msg(rc));
            return rc == result::DONE;
        }
        else
        {
            throw error("Database closed");
        }
    }

    void reset()
    {
        auto locked = db.lock();
        if(locked)
        {
            auto rc = stmt->reset();
            if(rc != result::OK)
                throw error(locked->err_msg(rc));
        }
        else
        {
            throw error("Database closed");
        }
    }

  private:
    template <typename T, typename... Args>
    void bind_(int current, T first, const Args &... rest)
    {
        auto rc = stmt->bind(current, first);
        if(rc != result::OK)
            throw error(db.lock()->err_msg(rc));
        bind_(current + 1, rest...);
    }

    void bind_(int)
    {
        // catch the end case
        return;
    }

    template <typename T, size_t... Is>
    T row_(std::index_sequence<Is...>)
    {
        return T(get_column<typename std::tuple_element_t<Is, T>>(Is)...);
    }

    template <typename T>
    std::enable_if_t<is_optional<T>::value, T> get_column(int current)
    {
        return get_column_<T>(stmt, current);
    }

    template <typename T>
    std::enable_if_t<!(is_optional<T>::value), T> get_column(int current)
    {
        return get_column_<optional<T>>(stmt, current).value_or(T());
    }

    database_internal::stmt *stmt;
    std::weak_ptr<database_internal> db;
};

class transaction
{
  public:
    transaction(std::weak_ptr<database_internal> db) : db(db)
    {
        auto s = std::make_unique<statement_internal>("BEGIN", db);
        s->step();
    }

    ~transaction() noexcept
    {
        try
        {
            if(!commited)
            {
                auto s = std::make_unique<statement_internal>("ROLLBACK", db);
                s->step();
            }
        }
        catch(const error &e)
        {
            assert(false);
        }
    }

    void commit()
    {
        if(!commited)
        {
            auto s = std::make_unique<statement_internal>("COMMIT", db);
            s->step();
            commited = true;
        }
    }

  private:
    bool commited;
    std::weak_ptr<database_internal> db;
};

template <typename... TS>
class query
{
  public:
    class iterator
    {
      public:
        iterator(query &q, bool done = false) : done(done), q(q) {}

        std::tuple<TS...> operator*() { return q.row(); }

        iterator &operator++()
        {
            done = q.stmt->step();
            return *this;
        }

        bool operator!=(const iterator &rhs) { return done != rhs.done; }

      private:
        bool done;
        query &q;
    };

    query(const std::string &sql, std::weak_ptr<database_internal> db)
        : stmt(std::make_unique<statement_internal>(sql, db))
    {
    }

    template <typename... Args>
    void bind(const Args &... args)
    {
        stmt->bind(args...);
    }

    std::tuple<TS...> row() { return stmt->row<TS...>(); }

    iterator begin()
    {
        stmt->reset();
        stmt->step();
        return iterator(*this);
    }

    iterator end() { return iterator(*this, true); }

  private:
    std::unique_ptr<statement_internal> stmt;
};

class statement
{
  public:
    statement(const std::string &sql, std::weak_ptr<database_internal> db)
        : stmt(std::make_unique<statement_internal>(sql, db))
    {
    }

    statement &operator++()
    {
        // no-op.
        return *this;
    }

    template <typename... TS>
    statement &operator=(const std::tuple<TS...> &rhs)
    {
        stmt->reset();
        apply(&statement_internal::bind<TS...>, stmt, rhs);
        stmt->step();
        return *this;
    }

  private:
    std::unique_ptr<statement_internal> stmt;
};

class database
{
  public:
    database() : database(":memory:", SQLITE_OPEN_READWRITE) {}

    database(const std::string &path, int flags)
        : db(std::make_shared<database_internal>(path, flags))
    {
    }

    std::unique_ptr<transaction> start_transaction()
    {
        return std::make_unique<transaction>(db);
    }

    template <typename... TS>
    std::unique_ptr<query<TS...>> prepare(const std::string &sql)
    {
        return std::make_unique<query<TS...>>(sql, db);
    }

    std::unique_ptr<statement> prepare_stmt(const std::string &sql)
    {
        return std::make_unique<statement>(sql, db);
    }

    template <typename... Args>
    void execute(const std::string &sql, Args &... args)
    {
        auto s = prepare_stmt(sql);
        *s = std::make_tuple(args...);
    }

  private:
    std::shared_ptr<database_internal> db;
};
};
