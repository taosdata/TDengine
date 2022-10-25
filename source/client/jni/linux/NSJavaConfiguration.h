/*
 * NSJavaConfiguration.h
 *
 * Copyright (c) 1997-2001, Apple Computer, Inc.
 * All Rights Reserved.
 *
 * LaurentR- April, 2000
 *    - added:
 *             NSDefaultJavaLibraryKey
 *             NSDefaultJavaDebugLibraryKey
 *             NSDefaultObjCJavaLibraryKey
 *             NSDefaultObjCJavaDebugLibraryKey
 *             NSJavaVMArgumentsKey
 */

#import <Foundation/Foundation.h>

// The configuration dictionary contains a set of vendor-specific key/value
// pairs and a set of default key/value pairs.  If no vendor is specified,
// NSJavaConfiguration uses the NSDefaultJavaVendorKey key to determine which
// vendor-specific dictionary should be searched before the top-level dictionary// is searched.  eg.:
/*
    {
	Vendor = sun;
	default = {
	    DefaultClasspath = "/NextLibrary/Java";
	};
	next = {
	    Compiler = "/usr/bin/javac";
	    VM = "/usr/bin/java";
	};
	sun = {
	    Compiler = "/NextLibrary/JDK/bin/javac";
	    VM = "/NextLibrary/JDK/bin/java";
	};
    }
*/
// In this case, if no vendor is specified, the `sun' mappings will be searched
// first.  The value for `VM' would be "/NextLibrary/JDK/bin/java" and the value
// for `DefaultClasspath' would be "/NextLibrary/Java".
//
// This search patter is applied to three dictionaries, in order:
//    - the JavaConfiguration dictionary in the defaults for the application
//    - the dictionary in the "JavaConfiguration" domain of the user defaults
//    - the configuration file (/NextLibrary/Java/JavaConfig.plist).
// This permits per-application, per-user and per-system specifications.


extern NSString *NSDefaultJavaVendorKey DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

extern NSString *NSDefaultJavaVMKey DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
extern NSString *NSDefaultJavaCompilerKey DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
extern NSString *NSDefaultJavaClassPathKey DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
extern NSString *NSDefaultJavaLibraryKey DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
extern NSString *NSDefaultJavaDebugLibraryKey DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
extern NSString *NSDefaultObjCJavaLibraryKey DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
extern NSString *NSDefaultObjCJavaDebugLibraryKey DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
extern NSString *NSJavaVMArgumentsKey DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;


@interface NSJavaConfiguration : NSObject
{
    NSString *_vendorName;
}

+ (NSJavaConfiguration *) defaultConfiguration DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

+ (NSJavaConfiguration *) configurationForVendor:(NSString *)vendorName DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
+ (NSArray *) vendorNames DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

- init DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
- initWithVendor:(NSString *)vendorName DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
- (NSString *) vendorName DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

- valueForKey:(NSString *)keyName DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
- valueForKey:(NSString *)keyName expandEnvironmentVariables:(BOOL)flag DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;

@end

