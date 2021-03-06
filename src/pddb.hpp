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

#pragma once

#include <cassert>
#include <stdexcept>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>

extern "C" {
#include <sqlite3.h>
}

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

class statement
{
  public:
    template <class... TS>
    class iterator
        : public std::iterator<std::input_iterator_tag, std::tuple<TS...>>
    {
      public:
        iterator() : stmt(nullptr), rc(result::DONE) {}

        explicit iterator(statement *stmt) : stmt(stmt)
        {
            stmt->reset();
            rc = stmt->step();
        }

        std::tuple<TS...> operator*() { return stmt->row<TS...>(); }

        iterator &operator++()
        {
            rc = stmt->step();
            return *this;
        }

        bool operator!=(const iterator &rhs) { return this->rc != rhs.rc; }

      private:
        statement *stmt;
        result rc;
    };

    template <class... TS>
    class rows
    {
      public:
        rows(statement *stmt) : stmt(stmt){};

        statement::iterator<TS...> begin()
        {
            return statement::iterator<TS...>(this->stmt);
        }

        statement::iterator<TS...> end()
        {
            return statement::iterator<TS...>();
        }

      private:
        statement *stmt;
    };

    statement(sqlite3_stmt *stmt) : stmt(stmt) {}

    ~statement() { sqlite3_finalize(stmt); }

    statement &reset()
    {
        auto rc = result(sqlite3_reset(stmt));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(sqlite3_db_handle(stmt)));
        return *this;
    }

    statement &clear()
    {
        auto rc = result(sqlite3_clear_bindings(stmt));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(sqlite3_db_handle(stmt)));
        return *this;
    }

    statement &bind(int32_t i, int index = 1)
    {
        auto rc = result(sqlite3_bind_int(stmt, index, i));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(sqlite3_db_handle(stmt)));

        return *this;
    }

    statement &bind(int64_t i, int index = 1)
    {
        auto rc = result(sqlite3_bind_int64(stmt, index, i));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(sqlite3_db_handle(stmt)));

        return *this;
    }

    statement &bind(double d, int index = 1)
    {
        auto rc = result(sqlite3_bind_double(stmt, index, d));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(sqlite3_db_handle(stmt)));

        return *this;
    }

    statement &bind(const std::string &s, int index = 1)
    {
        auto rc = result(
            sqlite3_bind_text(stmt, index, s.c_str(), -1, SQLITE_TRANSIENT));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(sqlite3_db_handle(stmt)));
        return *this;
    }

    result step()
    {
        auto rc = result(sqlite3_step(stmt));
        if(rc < result::ROW)
            throw error(sqlite3_errmsg(sqlite3_db_handle(stmt)));
        return rc;
    }

    template <typename T>
    T get_column(int column);

    template <class... TS>
    statement::rows<TS...> data()
    {
        return statement::rows<TS...>(this);
    }

    template <class... TS>
    std::tuple<TS...> row()
    {
        auto count = static_cast<std::size_t>(sqlite3_column_count(stmt));

        if(count < sizeof...(TS))
            throw error("Insufficient columns requested.");
        else if(count > sizeof...(TS))
            throw error("Excess columns requested.");

        int i = 0;
        return std::make_tuple(get_column<TS>(i++)...);
    }

  private:
    statement(const statement &) = delete;
    statement &operator=(const statement &) = delete;

    sqlite3_stmt *stmt;
};

template <>
int statement::get_column(int column)
{
    return sqlite3_column_int(stmt, column);
}

template <>
double statement::get_column(int column)
{
    return sqlite3_column_double(stmt, column);
}

template <>
std::string statement::get_column(int column)
{
    return std::string(
        reinterpret_cast<const char *>(sqlite3_column_text(stmt, column)),
        sqlite3_column_bytes(stmt, column));
}

class database;

class transaction
{
    friend class database;

  public:
    transaction(database &db);
    ~transaction() noexcept;

    void commit();

  private:
    bool commited;
    database &db;
};

class database
{
  public:
    database() : database(":memory:", SQLITE_OPEN_READWRITE) {}

    database(const std::string &path, int flags)
    {
        auto rc = result(sqlite3_open_v2(path.c_str(), &db, flags, nullptr));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(db));
    }

    ~database() { sqlite3_close_v2(db); }

    std::unique_ptr<pddb::transaction> transaction()
    {
        return std::make_unique<pddb::transaction>(*this);
    }

    std::unique_ptr<statement> prepare(const std::string &sql)
    {
        sqlite3_stmt *stmt;

        auto rc =
            result(sqlite3_prepare_v2(db, sql.data(), -1, &stmt, nullptr));
        if(rc != result::OK)
            throw error(sqlite3_errmsg(db));

        return std::make_unique<statement>(stmt);
    }

    result execute(const std::string &sql)
    {
        auto stmt = prepare(sql);
        return result(stmt->step());
    }

  private:
    database(const database &) = delete;
    database &operator=(const database &) = delete;

    sqlite3 *db;
};

transaction::transaction(database &db) : commited(false), db(db)
{
    db.execute("BEGIN");
}

transaction::~transaction() noexcept
{
    try
    {
        if(!commited)
        {
            db.execute("ROLLBACK");
        }
    }
    catch(const error &e)
    {
        assert(false);
    }
}

void transaction::commit()
{
    if(!commited)
    {
        db.execute("COMMIT");
        commited = true;
    }
}
}
