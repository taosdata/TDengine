//
//  jawt_md.h
//  Copyright (c) 2002-2010 Apple Inc. All rights reserved.
//

#ifndef _JAVASOFT_JAWT_MD_H_
#define _JAVASOFT_JAWT_MD_H_

#include "jawt.h"

#import <AppKit/NSView.h>
#import <QuartzCore/CALayer.h>


#ifdef __cplusplus
extern "C" {
#endif

/*
 * JAWT on Mac OS X has two rendering models; legacy NSView, and CALayer.
 *
 * The CALayer based model returns an object conforming to the JAWT_SurfaceLayers
 * protocol in it's JAWT_DrawingSurfaceInfo->platformInfo pointer. A CALayer
 * assigned to the "layer" property overlays the rectangle of the java.awt.Component.
 * The client CALayer traces the rect of the Java component as it is moved and resized.
 * 
 * If there is a superlayer for the entire window, it is also accessible via
 * the "windowLayer" property. This layer is useful for embedding the Java
 * window in other layer graphs.
 *
 *
 * The legacy NSView model provides raw access to the NSView hierarchy which
 * mirrors the Java component hierarchy. The legacy NSView drawing model is deprecated,
 * and will not be available in future versions of Java for Mac OS X.
 * 
 * Clients can opt-into the CALayer model by OR'ing the JAWT_MACOSX_USE_CALAYER into the requested JAWT version.
 * 
 *    JAWT awt;
 *    awt.version = JAWT_VERSION_1_4 | JAWT_MACOSX_USE_CALAYER;
 *    jboolean success = JAWT_GetAWT(env, &awt);
 *
 * Future versions of Java for Mac OS X will only support the CALayer model,
 * and will not return a JAWT_MacOSXDrawingSurfaceInfo struct.
 */

#define JAWT_MACOSX_USE_CALAYER 0x80000000

// CALayer-based rendering 
@protocol JAWT_SurfaceLayers
@property (readwrite, retain) CALayer *layer;
@property (readonly) CALayer *windowLayer;
@end


// Legacy NSView-based rendering
typedef struct JAWT_MacOSXDrawingSurfaceInfo {
    NSView *cocoaViewRef; // the view is guaranteed to be valid only for the duration of Component.paint method
}
JAWT_MacOSXDrawingSurfaceInfo;


#ifdef __cplusplus
}
#endif

#endif /* !_JAVASOFT_JAWT_MD_H_ */
