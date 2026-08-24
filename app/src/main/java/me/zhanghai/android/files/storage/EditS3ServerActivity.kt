/*
 * Copyright (c) 2026 Hartraft
 * All Rights Reserved.
 */

package me.zhanghai.android.files.storage

import android.os.Bundle
import android.view.View
import androidx.fragment.app.commit
import me.zhanghai.android.files.app.AppActivity
import me.zhanghai.android.files.util.args
import me.zhanghai.android.files.util.putArgs

class EditS3ServerActivity : AppActivity() {
    private val args by args<Args>()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        findViewById<View>(android.R.id.content)
        if (savedInstanceState == null) {
            supportFragmentManager.commit {
                add(android.R.id.content, AddS3ServerDialogFragment.newInstance(args.server))
            }
        }
    }

    @kotlinx.parcelize.Parcelize
    class Args(val server: S3Server) : me.zhanghai.android.files.util.ParcelableArgs
}
