/*
Classes for exceptions
Note: All exceptions thrown by Node.js or the JavaScript runtime will be instances of Error.
https://nodejs.org/api/errors.html#errors_exceptions_vs_errors

*/

class TDError extends Error {
  constructor(args) {
    super(args)
    this.name = "TDError";
  }
}
class Warning extends Error {
  // Exception raised for important warnings like data truncations while inserting.
  constructor(args) {
    super(args)
    this.name = "Warning";
  }
}
class InterfaceError extends TDError {
  // Exception raised for errors that are related to the database interface rather than the database itself.
  constructor(args) {
    super(args)
    this.name = "TDError.InterfaceError";
  }
}
class DatabaseError extends TDError {
  // Exception raised for errors that are related to the database.
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError";
  }
}
class DataError extends DatabaseError {
  // Exception raised for errors that are due to problems with the processed data like division by zero, numeric value out of range.
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.DataError";
  }
}
class OperationalError extends DatabaseError {
  // Exception raised for errors that are related to the database's operation and not necessarily under the control of the programmer
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.OperationalError";
  }
}
class IntegrityError extends DatabaseError {
  // Exception raised when the relational integrity of the database is affected.
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.IntegrityError";
  }
}
class InternalError extends DatabaseError {
  // Exception raised when the database encounters an internal error.
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.InternalError";
  }
}
class ProgrammingError extends DatabaseError {
  // Exception raised for programming errors.
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.ProgrammingError";
  }
}
class NotSupportedError extends DatabaseError {
  // Exception raised in case a method or database API was used which is not supported by the database.
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.NotSupportedError";
  }
}

module.exports = {
  TDError, Warning, InterfaceError, DatabaseError, DataError, OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError
};
