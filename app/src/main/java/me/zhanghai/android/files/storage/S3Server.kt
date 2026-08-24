/*
 * Copyright (c) 2026 Hartraft
 * All Rights Reserved.
 */

package me.zhanghai.android.files.storage

import android.content.Context
import android.content.Intent
import androidx.annotation.DrawableRes
import java8.nio.file.Path
import java8.nio.file.spi.FileSystemProvider
import kotlinx.parcelize.Parcelize
import me.zhanghai.android.files.R
import me.zhanghai.android.files.provider.s3.S3FileSystemProvider
import me.zhanghai.android.files.util.createIntent
import me.zhanghai.android.files.util.putArgs
import kotlin.random.Random

@Parcelize
class S3Server(
    override val id: Long,
    override val customName: String?,
    val endpoint: String,
    val accessKey: String,
    val secretKey: String,
    val bucket: String,
    val relativePath: String
) : Storage() {
    constructor(
        id: Long?,
        customName: String?,
        endpoint: String,
        accessKey: String,
        secretKey: String,
        bucket: String,
        relativePath: String
    ) : this(
        id ?: Random.nextLong(),
        customName,
        endpoint.trimEnd('/'),
        accessKey,
        secretKey,
        bucket,
        relativePath.trim('/')
    )

    override val iconRes: Int
        @DrawableRes
        get() = R.drawable.computer_icon_white_24dp

    override fun getDefaultName(context: Context): String =
        if (relativePath.isNotEmpty()) "$bucket/$relativePath" else bucket

    override val description: String
        get() = endpoint

    override val path: Path
        get() {
            FileSystemProvider.installProvider(S3FileSystemProvider)
            S3FileSystemProvider.register(this)
            return S3FileSystemProvider.getPath(
                java.net.URI.create("s3://$id/${relativePath.ifEmpty { "" }}")
            )
        }

    override fun createEditIntent(): Intent =
        EditS3ServerActivity::class.createIntent().putArgs(EditS3ServerActivity.Args(this))

    override fun toString(): String = "S3Server($endpoint/$bucket)"
}
