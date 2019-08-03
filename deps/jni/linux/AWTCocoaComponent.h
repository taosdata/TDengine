//
//  AWTCocoaComponent.h
//
//  Copyright (c) 2003 Apple Computer Inc. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <JavaVM/jni.h>

// This is implemented by a com.apple.eawt.CocoaComponent. It receives messages
//  from java safely on the AppKit thread. See the com.apple.eawt.CocoaComponent
//  java documentation for more information.
@protocol AWTCocoaComponent
-(void)awtMessage:(jint)messageID message:(jobject)message env:(JNIEnv*)env DEPRECATED_IN_MAC_OS_X_VERSION_10_6_AND_LATER;
@end
