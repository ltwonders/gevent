// Package gevent is used for decoupling domain event and improving performance
// main usage scenarios:
// 1. side business separating from main workflow, like system log etc.
// 2. delayed event to check result except polling, like checking pay result status
// 3. de-couple layer event without cycling call
// 4. notify downstream while domain event happening,like after pay finished etc./*
package gevent
