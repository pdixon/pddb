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
    template <typename... TS>
    friend class statement;
    friend class transaction;

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
        void clear(db &db)
        {
            auto rc = result(sqlite3_clear_bindings(stmt_));
            if(rc != result::OK)
                throw error(sqlite3_errmsg(db));
        }
        bool step(db &db)
        {
            auto rc = result(sqlite3_step(stmt_));
            if(rc < result::ROW)
                throw error(sqlite3_errmsg(db));
            return rc == result::DONE;
        }
        void reset(db &db)
        {
            auto rc = result(sqlite3_reset(stmt_));
            if(rc != result::OK)
                throw error(sqlite3_errmsg(db));
        }
        void bind(db &db, int column, int value)
        {
            auto rc = result(sqlite3_bind_int(stmt_, column, value));
            if(rc != result::OK)
                throw error(sqlite3_errmsg(db));
        }
        void bind(db &db, int column, int64_t value)
        {
            auto rc = result(sqlite3_bind_int64(stmt_, column, value));
            if(rc != result::OK)
                throw error(sqlite3_errmsg(db));
        }
        void bind(db &db, int column, double value)
        {
            auto rc = result(sqlite3_bind_double(stmt_, column, value));
            if(rc != result::OK)
                throw error(sqlite3_errmsg(db));
        }
        void bind(db &db, int column, std::string value)
        {
            auto rc = result(sqlite3_bind_text(stmt_, column, value.c_str(), -1,
                                               SQLITE_TRANSIENT));
            if(rc != result::OK)
                throw error(sqlite3_errmsg(db));
        }
        void bind(db &db, int column, std::vector<uint8_t> value)
        {
            auto rc = result(sqlite3_bind_blob(stmt_, column, value.data(),
                                               value.size(), SQLITE_TRANSIENT));
            if(rc != result::OK)
                throw error(sqlite3_errmsg(db));
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

    void execute(const std::string &sql)
    {
        auto s = prepare(sql);
        auto rc = result(sqlite3_step(*s));
        if(rc != result::DONE)
            throw error(sqlite3_errmsg(*db_));
        stmt_finalize(s);
    }

  private:
    std::unordered_set<std::unique_ptr<stmt>> statements;
    std::unique_ptr<db> db_;
};

template <typename T>
T get_column(database_internal::stmt *s, int current);

template <>
int get_column(database_internal::stmt *s, int current)
{
    return s->get_int(current).value_or(0);
}

#if 0
template <>
optional<int> get_column(database_internal::stmt *s, int current)
{
    return s->get_int(current);
}
#endif

template <>
int64_t get_column(database_internal::stmt *s, int current)
{
    return s->get_int(current).value_or(0);
}

template <>
optional<int64_t> get_column(database_internal::stmt *s, int current)
{
    return s->get_int(current);
}

template <>
double get_column(database_internal::stmt *s, int current)
{
    return s->get_double(current).value_or(0.0);
}

template <>
optional<double> get_column(database_internal::stmt *s, int current)
{
    return s->get_double(current);
}

template <>
std::string get_column(database_internal::stmt *s, int current)
{
    return s->get_string(current).value_or("");
}

template <>
optional<std::string> get_column(database_internal::stmt *s, int current)
{
    return s->get_string(current);
}

template <>
std::vector<uint8_t> get_column(database_internal::stmt *s, int current)
{
    return s->get_blob(current).value_or(std::vector<uint8_t>());
}

template <>
optional<std::vector<uint8_t>> get_column(database_internal::stmt *s,
                                          int current)
{
    return s->get_blob(current);
}

class transaction
{
  public:
    transaction(std::weak_ptr<database_internal> db) : db(db)
    {
        db.lock()->execute("BEGIN");
    }

    ~transaction() noexcept
    {
        try
        {
            if(!commited)
            {
                db.lock()->execute("ROLLBACK");
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
            db.lock()->execute("COMMIT");
            commited = true;
        }
    }

  private:
    bool commited;
    std::weak_ptr<database_internal> db;
};

template <typename... TS>
class statement
{
  public:
    class iterator
    {
      public:
        iterator(statement &s, bool end = false) : s_(s), end_(end) {}

        std::tuple<TS...> operator*() { return s_.row(); }

        iterator &operator++()
        {
            end_ = s_.step();
            return *this;
        }

        bool operator!=(const iterator &rhs) { return end_ != rhs.end_; }

      private:
        statement &s_;
        bool end_;
    };

    statement(database_internal::stmt *s, std::weak_ptr<database_internal> db)
        : stmt_(s), db(db)
    {
    }

    ~statement()
    {
        auto locked = db.lock();
        if(locked)
        {
            locked->stmt_finalize(stmt_);
        }
    }

    template <typename... Args>
    void bind(const Args &... args)
    {
        auto locked = db.lock();
        if(locked)
        {
            stmt_->clear(*locked->db_);
            bind_(locked, 1, args...);
        }
        else
        {
            throw error("Database closed");
        }
    }

    bool step()
    {
        auto locked = db.lock();
        if(locked)
        {
            return stmt_->step(*locked->db_);
        }
        else
        {
            throw error("Database closed");
        }
    }

    std::tuple<TS...> row()
    {
        int i = 0;
        return std::make_tuple(get_column<TS>(stmt_, i++)...);
    }

    iterator begin()
    {
        auto locked = db.lock();
        if(locked)
        {
            stmt_->reset(*locked->db_);
            return iterator{*this};
        }
        else
        {
            throw error("Database closed");
        }
    }

    iterator end() { return iterator{*this, true}; }

  private:
    template <typename T, typename... Args>
    void bind_(std::shared_ptr<database_internal> db_, int current, T first,
               const Args &... rest)
    {
        stmt_->bind(*db_->db_, current, first);
        bind_(db_, current + 1, rest...);
    }

    void bind_(std::shared_ptr<database_internal>, int)
    {
        // catch the end case
        return;
    }

    database_internal::stmt *stmt_;
    std::weak_ptr<database_internal> db;
};

class database
{
  public:
    database()
        : db(std::make_shared<database_internal>(":memory:",
                                                 SQLITE_OPEN_READWRITE))
    {
    }
    database(const std::string &path, int flags)
        : db(std::make_shared<database_internal>(path, flags))
    {
    }
    std::unique_ptr<transaction> start_transaction()
    {
        return std::make_unique<transaction>(db);
    }

    template <typename... TS>
    std::unique_ptr<statement<TS...>> prepare(const std::string &sql)
    {
        return std::make_unique<statement<TS...>>(db->prepare(sql), db);
    }

    void execute(const std::string& sql)
    {
        db->execute(sql);
    }

  private:
    std::shared_ptr<database_internal> db;
};
};

