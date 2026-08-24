/*
 * Copyright (c) 2019 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package me.zhanghai.android.files.storage

import android.os.Bundle
import android.view.View
import androidx.fragment.app.commit
import me.zhanghai.android.files.app.AppActivity

class AddStorageDialogActivity : AppActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        // Calls ensureSubDecor().
        findViewById<View>(android.R.id.content)
        if (savedInstanceState == null) {
            supportFragmentManager.commit {
                val server = intent.getParcelableExtra<S3Server>(EXTRA_S3_SERVER)
                if (server != null) {
                    add(android.R.id.content, AddS3ServerDialogFragment.newInstance(server))
                } else {
                    add<AddStorageDialogFragment>(AddStorageDialogFragment::class.java.name)
                }
            }
        }
    }

    companion object {
        const val EXTRA_S3_SERVER = "s3_server"
    }
}
