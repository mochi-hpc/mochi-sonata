#include "unqlite/unqlite.h"
#include <abt.h>
#include <cstdlib>
#include <iostream>

#define SXMUTEX_TYPE_FAST      1
#define SXMUTEX_TYPE_RECURSIVE 2
#define SXMUTEX_TYPE_STATIC_1  3
#define SXMUTEX_TYPE_STATIC_2  4
#define SXMUTEX_TYPE_STATIC_3  5
#define SXMUTEX_TYPE_STATIC_4  6
#define SXMUTEX_TYPE_STATIC_5  7
#define SXMUTEX_TYPE_STATIC_6  8

struct ABTSyMutex
{
    ABT_mutex sMutex;
    unsigned int nType;
};
    
static ABTSyMutex aStaticMutexes[] = {
    {ABT_MUTEX_NULL, SXMUTEX_TYPE_STATIC_1}, 
    {ABT_MUTEX_NULL, SXMUTEX_TYPE_STATIC_2}, 
    {ABT_MUTEX_NULL, SXMUTEX_TYPE_STATIC_3}, 
    {ABT_MUTEX_NULL, SXMUTEX_TYPE_STATIC_4}, 
    {ABT_MUTEX_NULL, SXMUTEX_TYPE_STATIC_5}, 
    {ABT_MUTEX_NULL, SXMUTEX_TYPE_STATIC_6}
};

static int ABTMutexGlobalInit(void) {
    for(unsigned i=0; i < 6; i++) {
        ABT_mutex_create(&aStaticMutexes[i].sMutex);
    }
    return 0;
}

static void ABTMutexGlobalRelease(void) {
    for(unsigned i=0; i < 6; i++) {
        ABT_mutex_free(&aStaticMutexes[i].sMutex);
    }
}

static SyMutex* ABTMutexNew(int nType)
{
    if(nType == SXMUTEX_TYPE_FAST || nType == SXMUTEX_TYPE_RECURSIVE) {
        ABTSyMutex *pMutex = (ABTSyMutex *)malloc(sizeof(ABTSyMutex));
        pMutex->nType = nType;
        if(!pMutex){
            return nullptr;
        }
        if(nType == SXMUTEX_TYPE_RECURSIVE) {
            ABT_mutex_attr attr;
            ABT_mutex_attr_create(&attr);
            ABT_mutex_attr_set_recursive(attr, ABT_TRUE);
            ABT_mutex_create_with_attr(attr, &(pMutex->sMutex));
            ABT_mutex_attr_free(&attr);
        } else {
            ABT_mutex_create(&(pMutex->sMutex));
        }
        return reinterpret_cast<SyMutex*>(pMutex);
    } else {
        ABTSyMutex *pMutex = nullptr;
        /* Use a pre-allocated static mutex */
        if(nType > SXMUTEX_TYPE_STATIC_6) {
            nType = SXMUTEX_TYPE_STATIC_6;
        }
        pMutex = &aStaticMutexes[nType - 3];
        return reinterpret_cast<SyMutex*>(pMutex);
    }
}

static void ABTMutexRelease(SyMutex *arg)
{
    auto pMutex = reinterpret_cast<ABTSyMutex*>(arg);
    if(pMutex->nType == SXMUTEX_TYPE_FAST || pMutex->nType == SXMUTEX_TYPE_RECURSIVE) {
        ABT_mutex_free(&pMutex->sMutex);
        free(pMutex);
    }
}

static void ABTMutexEnter(SyMutex *arg)
{
    auto pMutex = reinterpret_cast<ABTSyMutex*>(arg);
    ABT_mutex_lock(pMutex->sMutex);
}

static int ABTMutexTryEnter(SyMutex *arg)
{
    auto pMutex = reinterpret_cast<ABTSyMutex*>(arg);
    int ret = ABT_mutex_trylock(pMutex->sMutex);
    if(ABT_ERR_MUTEX_LOCKED == ret) {
        ABT_thread_yield();
    }
    return ret == ABT_SUCCESS ? SXRET_OK : SXERR_BUSY;
}

static void ABTMutexLeave(SyMutex *arg)
{
    auto pMutex = reinterpret_cast<ABTSyMutex*>(arg);
    ABT_mutex_unlock(pMutex->sMutex);
}

static const SyMutexMethods sABTMutexMethods = {
    ABTMutexGlobalInit,
    ABTMutexGlobalRelease,
    ABTMutexNew,
    ABTMutexRelease,
    ABTMutexEnter,
    ABTMutexTryEnter,
    ABTMutexLeave
};

const SyMutexMethods* ExportUnqliteArgobotsMutexMethods(void)
{
    return &sABTMutexMethods;
}
