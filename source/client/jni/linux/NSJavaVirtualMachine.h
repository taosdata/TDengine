/*
 * NSJavaVirtualMachine.h
 *
 * Copyright (c) 1997-2001, Apple Computer, Inc.
 * All Rights Reserved.
 */

#import <Foundation/Foundation.h>

@interface NSJavaVirtualMachine : NSObject
{
@private
    void *_vmdata;
}


// Returns the default virtual machine - if necessary, calls alloc + init

+ (id) defaultVirtualMachine DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

// Returns a class path.  First checks NSProcessInfo for an environment variable
// called CLASSPATH and if that doesn't exist, uses NSJavaConfiguration to find
// the default class path.

+ (NSString *) defaultClassPath DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

// Note that any NSThreads spawned after this method returns will automatically 
// be attached to the virtual machine.  Likewise, it is not necessary to attach
// the thread that is actually creating the virtual machine.  If you spawn a
// thread before creating the virtual machine, or if you use the cthread/pthread
// or any other non-NSThread api for creating a thread, you must explicitly
// attach those threads before messaging any Java object from that thread.
// This is most easily done by using the -attachCurrentThread method.
// Use -detachCurrentThread to detach explicitly attached threads when done.

- initWithClassPath:(NSString *)classPath DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

- (void) attachCurrentThread DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
- (void) detachCurrentThread DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

- (Class)findClass:(NSString *)className DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
- (Class)defineClass:(NSData *)javaClassData withName:(NSString *)className DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

@end


@interface NSObject (InstantiatingJavaObjects)

// Instantiating java objects for when no -init/constructor mapping works.
// The class these methods are invoked on *must* be a class returned by the
// -findClass: method (or NSClassFromString() function), otherwise
// NSInvalidJavaClassException is raised. The signature is specified using the
// rather counter-intuitive format defined by the Java Virtual Machine
// specification.  Try looking in JavaVM/vm-interface.h for help.

+ (id) newWithSignature:(NSString *)signature, ... DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
+ (id) newWithSignature:(NSString *)signature arguments:(va_list)args DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

@end

extern NSString *NSInvalidJavaClassException DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
