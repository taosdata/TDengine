/*
 * @(#)jawt.h	1.11 05/11/17
 *
 * Copyright 2006 Sun Microsystems, Inc. All rights reserved.
 * SUN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

#ifndef _JAVASOFT_JAWT_H_
#define _JAVASOFT_JAWT_H_

#include "jni.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * JAWT_Rectangle
 * Structure for a native rectangle.
 */
typedef struct jawt_Rectangle {
    jint x;
    jint y;
    jint width;
    jint height;
} JAWT_Rectangle;

struct jawt_DrawingSurface;

/*
 * JAWT_DrawingSurfaceInfo
 * Structure for containing the underlying drawing information of a component.
 */
typedef struct jawt_DrawingSurfaceInfo {
    /*
     * Pointer to the platform-specific information.  This can be safely
     * cast to a JAWT_Win32DrawingSurfaceInfo on Windows or a
     * JAWT_X11DrawingSurfaceInfo on Solaris.  See jawt_md.h for details.
     */
    void* platformInfo;
    /* Cached pointer to the underlying drawing surface */
    struct jawt_DrawingSurface* ds;
    /* Bounding rectangle of the drawing surface */
    JAWT_Rectangle bounds;
    /* Number of rectangles in the clip */
    jint clipSize;
    /* Clip rectangle array */
    JAWT_Rectangle* clip;
} JAWT_DrawingSurfaceInfo;

#define JAWT_LOCK_ERROR                 0x00000001
#define JAWT_LOCK_CLIP_CHANGED          0x00000002
#define JAWT_LOCK_BOUNDS_CHANGED        0x00000004
#define JAWT_LOCK_SURFACE_CHANGED       0x00000008

/*
 * JAWT_DrawingSurface
 * Structure for containing the underlying drawing information of a component.
 * All operations on a JAWT_DrawingSurface MUST be performed from the same
 * thread as the call to GetDrawingSurface.
 */
typedef struct jawt_DrawingSurface {
    /*
     * Cached reference to the Java environment of the calling thread.
     * If Lock(), Unlock(), GetDrawingSurfaceInfo() or
     * FreeDrawingSurfaceInfo() are called from a different thread,
     * this data member should be set before calling those functions.
     */
    JNIEnv* env;
    /* Cached reference to the target object */
    jobject target;
    /*
     * Lock the surface of the target component for native rendering.
     * When finished drawing, the surface must be unlocked with
     * Unlock().  This function returns a bitmask with one or more of the
     * following values:
     *
     * JAWT_LOCK_ERROR - When an error has occurred and the surface could not
     * be locked.
     *
     * JAWT_LOCK_CLIP_CHANGED - When the clip region has changed.
     *
     * JAWT_LOCK_BOUNDS_CHANGED - When the bounds of the surface have changed.
     *
     * JAWT_LOCK_SURFACE_CHANGED - When the surface itself has changed
     */
    jint (JNICALL *Lock)
        (struct jawt_DrawingSurface* ds);
    /*
     * Get the drawing surface info.
     * The value returned may be cached, but the values may change if
     * additional calls to Lock() or Unlock() are made.
     * Lock() must be called before this can return a valid value.
     * Returns NULL if an error has occurred.
     * When finished with the returned value, FreeDrawingSurfaceInfo must be
     * called.
     */
    JAWT_DrawingSurfaceInfo* (JNICALL *GetDrawingSurfaceInfo)
        (struct jawt_DrawingSurface* ds);
    /*
     * Free the drawing surface info.
     */
    void (JNICALL *FreeDrawingSurfaceInfo)
        (JAWT_DrawingSurfaceInfo* dsi);
    /* 
     * Unlock the drawing surface of the target component for native rendering.
     */
    void (JNICALL *Unlock)
        (struct jawt_DrawingSurface* ds);
} JAWT_DrawingSurface;

/*
 * JAWT
 * Structure for containing native AWT functions.
 */
typedef struct jawt {
    /*
     * Version of this structure.  This must always be set before
     * calling JAWT_GetAWT()
     */
    jint version;
    /*
     * Return a drawing surface from a target jobject.  This value
     * may be cached.
     * Returns NULL if an error has occurred.
     * Target must be a java.awt.Component (should be a Canvas
     * or Window for native rendering).
     * FreeDrawingSurface() must be called when finished with the
     * returned JAWT_DrawingSurface.
     */
    JAWT_DrawingSurface* (JNICALL *GetDrawingSurface)
        (JNIEnv* env, jobject target);
    /*
     * Free the drawing surface allocated in GetDrawingSurface.
     */
    void (JNICALL *FreeDrawingSurface)
        (JAWT_DrawingSurface* ds);
    /*
     * Since 1.4
     * Locks the entire AWT for synchronization purposes
     */
    void (JNICALL *Lock)(JNIEnv* env);
    /*
     * Since 1.4
     * Unlocks the entire AWT for synchronization purposes
     */
    void (JNICALL *Unlock)(JNIEnv* env);
    /*
     * Since 1.4
     * Returns a reference to a java.awt.Component from a native
     * platform handle.  On Windows, this corresponds to an HWND;
     * on Solaris and Linux, this is a Drawable.  For other platforms,
     * see the appropriate machine-dependent header file for a description.
     * The reference returned by this function is a local
     * reference that is only valid in this environment.
     * This function returns a NULL reference if no component could be
     * found with matching platform information.
     */
    jobject (JNICALL *GetComponent)(JNIEnv* env, void* platformInfo);

} JAWT;

/*
 * Get the AWT native structure.  This function returns JNI_FALSE if
 * an error occurs.
 */
_JNI_IMPORT_OR_EXPORT_ __attribute__((deprecated))
jboolean JNICALL JAWT_GetAWT(JNIEnv* env, JAWT* awt);

#define JAWT_VERSION_1_3 0x00010003
#define JAWT_VERSION_1_4 0x00010004

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !_JAVASOFT_JAWT_H_ */
