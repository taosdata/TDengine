export interface ExceptionSummary
{
    type: string
    message: string
    stack: string
    timestamp: Date
    innerExceptionType: string
    innerExceptionMessage: string
    innerExceptionStack: string
}