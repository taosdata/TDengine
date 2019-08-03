
/**
 * TDengine Error Class
 * @ignore
 */
class TDError extends Error {
  constructor(args) {
    super(args)
    this.name = "TDError";
  }
}
/** Exception raised for important warnings like data truncations while inserting.
 * @ignore
 */
class Warning extends Error {
  constructor(args) {
    super(args)
    this.name = "Warning";
  }
}
/** Exception raised for errors that are related to the database interface rather than the database itself.
 * @ignore
 */
class InterfaceError extends TDError {
  constructor(args) {
    super(args)
    this.name = "TDError.InterfaceError";
  }
}
/** Exception raised for errors that are related to the database.
 * @ignore
 */
class DatabaseError extends TDError {
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError";
  }
}
/** Exception raised for errors that are due to problems with the processed data like division by zero, numeric value out of range.
 * @ignore
 */
class DataError extends DatabaseError {
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.DataError";
  }
}
/** Exception raised for errors that are related to the database's operation and not necessarily under the control of the programmer
 * @ignore
 */
class OperationalError extends DatabaseError {
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.OperationalError";
  }
}
/** Exception raised when the relational integrity of the database is affected.
 * @ignore
 */
class IntegrityError extends DatabaseError {
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.IntegrityError";
  }
}
/** Exception raised when the database encounters an internal error.
 * @ignore
 */
class InternalError extends DatabaseError {
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.InternalError";
  }
}
/**  Exception raised for programming errors.
 * @ignore
 */
class ProgrammingError extends DatabaseError {
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.ProgrammingError";
  }
}
/** Exception raised in case a method or database API was used which is not supported by the database.
 * @ignore
 */
class NotSupportedError extends DatabaseError {
  constructor(args) {
    super(args)
    this.name = "TDError.DatabaseError.NotSupportedError";
  }
}

module.exports = {
  TDError, Warning, InterfaceError, DatabaseError, DataError, OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError
};
