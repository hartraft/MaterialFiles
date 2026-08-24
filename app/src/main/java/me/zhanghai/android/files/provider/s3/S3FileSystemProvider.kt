/*
 * Copyright (c) 2026 Hartraft
 * All Rights Reserved.
 */

package me.zhanghai.android.files.provider.s3

import android.os.Parcel
import android.os.Parcelable
import java8.nio.channels.SeekableByteChannel
import java8.nio.file.AccessMode
import java8.nio.file.CopyOption
import java8.nio.file.DirectoryStream
import java8.nio.file.FileStore
import java8.nio.file.FileSystem
import java8.nio.file.FileSystemAlreadyExistsException
import java8.nio.file.FileSystemNotFoundException
import java8.nio.file.LinkOption
import java8.nio.file.OpenOption
import java8.nio.file.Path
import java8.nio.file.PathMatcher
import java8.nio.file.ProviderMismatchException
import java8.nio.file.StandardOpenOption
import java8.nio.file.WatchEvent
import java8.nio.file.WatchKey
import java8.nio.file.WatchService
import java8.nio.file.attribute.BasicFileAttributeView
import java8.nio.file.attribute.BasicFileAttributes
import java8.nio.file.attribute.FileAttribute
import java8.nio.file.attribute.FileAttributeView
import java8.nio.file.attribute.FileTime
import java8.nio.file.spi.FileSystemProvider
import io.minio.CopyObjectArgs
import io.minio.CopySource
import io.minio.DownloadObjectArgs
import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.PutObjectArgs
import io.minio.RemoveObjectArgs
import io.minio.Result
import io.minio.StatObjectArgs
import io.minio.UploadObjectArgs
import io.minio.messages.Item
import me.zhanghai.android.files.provider.common.ByteString
import me.zhanghai.android.files.provider.common.ByteStringListPath
import me.zhanghai.android.files.provider.common.ByteStringListPathCreator
import me.zhanghai.android.files.provider.common.LocalWatchService
import me.zhanghai.android.files.provider.common.PathListDirectoryStream
import me.zhanghai.android.files.provider.common.PathObservable
import me.zhanghai.android.files.provider.common.PathObservableProvider
import me.zhanghai.android.files.provider.common.Searchable
import me.zhanghai.android.files.provider.common.UriAuthority
import me.zhanghai.android.files.provider.common.WalkFileTreeSearchable
import me.zhanghai.android.files.provider.common.WatchServicePathObservable
import me.zhanghai.android.files.provider.common.toByteString
import me.zhanghai.android.files.storage.S3Server
import java.io.File
import java.io.IOException
import java.net.URI

/**
 * S3-compatible storage provider for MaterialFiles.
 *
 * A registered S3Server is represented by a FileSystem. The URI authority is the
 * persisted server id, so credentials never appear in a Path URI.
 *
 * Object I/O is staged through a temporary local file. This deliberately keeps the
 * implementation compatible with MaterialFiles' existing Java NIO file-job pipeline.
 */
object S3FileSystemProvider : FileSystemProvider(), PathObservableProvider, Searchable {
    private const val SCHEME = "s3"
    private val fileSystems = mutableMapOf<Long, S3FileSystem>()
    private val servers = mutableMapOf<Long, S3Server>()
    private val lock = Any()

    fun register(server: S3Server) {
        synchronized(lock) { servers[server.id] = server }
    }

    fun unregister(server: S3Server) {
        synchronized(lock) {
            servers.remove(server.id)
            fileSystems.remove(server.id)?.close()
        }
    }

    override fun getScheme(): String = SCHEME

    override fun newFileSystem(uri: URI, env: Map<String, *>): FileSystem {
        val id = uri.requireId()
        synchronized(lock) {
            if (fileSystems[id] != null) throw FileSystemAlreadyExistsException(id.toString())
            return newFileSystemLocked(id)
        }
    }

    internal fun getOrNewFileSystem(id: Long): S3FileSystem =
        synchronized(lock) { fileSystems[id] ?: newFileSystemLocked(id) }

    private fun newFileSystemLocked(id: Long): S3FileSystem {
        val server = servers[id] ?: throw FileSystemNotFoundException(id.toString())
        return S3FileSystem(this, server).also { fileSystems[id] = it }
    }

    override fun getFileSystem(uri: URI): FileSystem =
        synchronized(lock) { fileSystems[uri.requireId()] }
            ?: throw FileSystemNotFoundException(uri.authority)

    internal fun removeFileSystem(fileSystem: S3FileSystem) {
        synchronized(lock) { fileSystems.remove(fileSystem.server.id) }
    }

    override fun getPath(uri: URI): Path {
        val fileSystem = getOrNewFileSystem(uri.requireId())
        return fileSystem.getPath(uri.path ?: throw IllegalArgumentException("URI must have a path"))
    }

    private fun URI.requireId(): Long {
        require(scheme.equals(SCHEME, ignoreCase = true)) { "URI scheme $scheme must be $SCHEME" }
        return authority?.toLongOrNull()
            ?: throw IllegalArgumentException("S3 URI authority must be a server id")
    }

    override fun newFileChannel(
        file: Path,
        options: Set<OpenOption>,
        vararg attributes: FileAttribute<*>
    ): java8.nio.channels.FileChannel {
        file as? S3Path ?: throw ProviderMismatchException(file.toString())
        throw UnsupportedOperationException()
    }

    override fun newByteChannel(
        file: Path,
        options: Set<OpenOption>,
        vararg attributes: FileAttribute<*>
    ): SeekableByteChannel {
        val path = file as? S3Path ?: throw ProviderMismatchException(file.toString())
        val writable = options.any { it == StandardOpenOption.WRITE || it == StandardOpenOption.APPEND }
        val temp = File.createTempFile("materialfiles-s3-", ".tmp")
        return try {
            if (writable && options.contains(StandardOpenOption.APPEND) && exists(path)) {
                path.fileSystem.client.downloadObject(
                    DownloadObjectArgs.builder()
                        .bucket(path.fileSystem.server.bucket)
                        .`object`(path.objectKey)
                        .filename(temp.absolutePath)
                        .build()
                )
            } else if (!writable) {
                path.fileSystem.client.downloadObject(
                    DownloadObjectArgs.builder()
                        .bucket(path.fileSystem.server.bucket)
                        .`object`(path.objectKey)
                        .filename(temp.absolutePath)
                        .build()
                )
            }
            val localOptions = if (writable) {
                setOf(StandardOpenOption.READ, StandardOpenOption.WRITE)
            } else {
                setOf(StandardOpenOption.READ)
            }
            val channel = java8.nio.file.Files.newByteChannel(
                java8.nio.file.Paths.get(temp.absolutePath), localOptions
            )
            if (writable) {
                if (options.contains(StandardOpenOption.APPEND)) channel.position(channel.size())
                else channel.truncate(0)
            }
            S3ByteChannel(channel, temp, path, writable)
        } catch (e: Exception) {
            temp.delete()
            throw IOException("Unable to open S3 object ${path.objectKey}", e)
        }
    }

    override fun newDirectoryStream(
        directory: Path,
        filter: DirectoryStream.Filter<in Path>
    ): DirectoryStream<Path> {
        val path = directory as? S3Path ?: throw ProviderMismatchException(directory.toString())
        val prefix = path.objectKey.let { if (it.isEmpty()) "" else "${it.trimEnd('/')}/" }
        val paths = mutableListOf<Path>()
        try {
            val results = path.fileSystem.client.listObjects(
                ListObjectsArgs.builder()
                    .bucket(path.fileSystem.server.bucket)
                    .prefix(prefix)
                    .recursive(false)
                    .build()
            )
            for (result: Result<Item> in results) {
                val item = result.get()
                var key = item.objectName()
                if (item.isDir) key = key.trimEnd('/')
                if (key.isEmpty() || key == path.objectKey) continue
                val relative = if (prefix.isEmpty()) key else key.removePrefix(prefix)
                if (relative.contains('/')) continue
                val child = path.resolve(relative)
                if (filter.accept(child)) paths += child
            }
        } catch (e: Exception) {
            throw IOException("Unable to list S3 directory ${path.objectKey}", e)
        }
        return PathListDirectoryStream(paths, filter)
    }

    override fun createDirectory(directory: Path, vararg attributes: FileAttribute<*>) {
        val path = directory as? S3Path ?: throw ProviderMismatchException(directory.toString())
        val key = path.objectKey.trimEnd('/') + "/"
        if (path.objectKey.isEmpty()) return
        try {
            path.fileSystem.client.putObject(
                PutObjectArgs.builder()
                    .bucket(path.fileSystem.server.bucket)
                    .`object`(key)
                    .stream(java.io.ByteArrayInputStream(ByteArray(0)), 0, -1)
                    .build()
            )
        } catch (e: Exception) {
            throw IOException("Unable to create S3 directory ${path.objectKey}", e)
        }
    }

    override fun delete(path: Path) {
        val s3Path = path as? S3Path ?: throw ProviderMismatchException(path.toString())
        try {
            if (isDirectory(s3Path)) {
                val prefix = s3Path.objectKey.trimEnd('/') + "/"
                val objects = s3Path.fileSystem.client.listObjects(
                    ListObjectsArgs.builder()
                        .bucket(s3Path.fileSystem.server.bucket)
                        .prefix(prefix)
                        .recursive(true)
                        .build()
                )
                for (result: Result<Item> in objects) {
                    s3Path.fileSystem.client.removeObject(
                        RemoveObjectArgs.builder()
                            .bucket(s3Path.fileSystem.server.bucket)
                            .`object`(result.get().objectName())
                            .build()
                    )
                }
                return
            }
            s3Path.fileSystem.client.removeObject(
                RemoveObjectArgs.builder()
                    .bucket(s3Path.fileSystem.server.bucket)
                    .`object`(s3Path.objectKey)
                    .build()
            )
        } catch (e: Exception) {
            throw IOException("Unable to delete S3 path ${s3Path.objectKey}", e)
        }
    }

    override fun copy(source: Path, target: Path, vararg options: CopyOption) {
        val from = source as? S3Path ?: throw ProviderMismatchException(source.toString())
        val to = target as? S3Path ?: throw ProviderMismatchException(target.toString())
        requireSameFileSystem(from, to)
        try {
            from.fileSystem.client.copyObject(
                CopyObjectArgs.builder()
                    .bucket(to.fileSystem.server.bucket)
                    .`object`(to.objectKey)
                    .source(
                        CopySource.builder()
                            .bucket(from.fileSystem.server.bucket)
                            .`object`(from.objectKey)
                            .build()
                    )
                    .build()
            )
        } catch (e: Exception) {
            throw IOException("Unable to copy S3 path ${from.objectKey}", e)
        }
    }

    override fun move(source: Path, target: Path, vararg options: CopyOption) {
        copy(source, target, *options)
        delete(source)
    }

    override fun isSameFile(path: Path, path2: Path): Boolean = path == path2

    override fun isHidden(path: Path): Boolean {
        path as? S3Path ?: throw ProviderMismatchException(path.toString())
        return false
    }

    override fun getFileStore(path: Path): FileStore {
        path as? S3Path ?: throw ProviderMismatchException(path.toString())
        throw UnsupportedOperationException()
    }

    override fun checkAccess(path: Path, vararg modes: AccessMode) {
        val s3Path = path as? S3Path ?: throw ProviderMismatchException(path.toString())
        if (modes.contains(AccessMode.EXECUTE)) throw UnsupportedOperationException()
        try {
            if (!isDirectory(s3Path)) {
                s3Path.fileSystem.client.statObject(
                    StatObjectArgs.builder()
                        .bucket(s3Path.fileSystem.server.bucket)
                        .`object`(s3Path.objectKey)
                        .build()
                )
            }
        } catch (e: Exception) {
            throw IOException("Unable to access S3 path ${s3Path.objectKey}", e)
        }
    }

    override fun <V : FileAttributeView> getFileAttributeView(
        path: Path,
        type: Class<V>,
        vararg options: LinkOption
    ): V? {
        if (!type.isAssignableFrom(BasicFileAttributeView::class.java)) return null
        @Suppress("UNCHECKED_CAST")
        return S3BasicFileAttributeView(path as? S3Path ?: throw ProviderMismatchException(path.toString())) as V
    }

    override fun <A : BasicFileAttributes> readAttributes(
        path: Path,
        type: Class<A>,
        vararg options: LinkOption
    ): A {
        if (!type.isAssignableFrom(BasicFileAttributes::class.java)) throw UnsupportedOperationException(type.toString())
        val s3Path = path as? S3Path ?: throw ProviderMismatchException(path.toString())
        @Suppress("UNCHECKED_CAST")
        return attributes(s3Path) as A
    }

    override fun readAttributes(
        path: Path,
        attributes: String,
        vararg options: LinkOption
    ): Map<String, Any> {
        val result = readAttributes(path, BasicFileAttributes::class.java, *options)
        return mapOf(
            "size" to result.size(),
            "creationTime" to result.creationTime(),
            "lastModifiedTime" to result.lastModifiedTime(),
            "lastAccessTime" to result.lastAccessTime(),
            "isRegularFile" to result.isRegularFile(),
            "isDirectory" to result.isDirectory(),
            "isSymbolicLink" to result.isSymbolicLink(),
            "isOther" to result.isOther()
        )
    }

    override fun setAttribute(path: Path, attribute: String, value: Any, vararg options: LinkOption) {
        throw UnsupportedOperationException()
    }

    override fun observe(path: Path, intervalMillis: Long): PathObservable =
        WatchServicePathObservable(path, intervalMillis)

    override fun search(directory: Path, query: String, intervalMillis: Long, listener: (List<Path>) -> Unit) =
        WalkFileTreeSearchable.search(directory, query, intervalMillis, listener)

    internal fun attributes(path: S3Path): S3Attributes {
        if (path.objectKey.isEmpty()) return S3Attributes.directory()
        return try {
            val stat = path.fileSystem.client.statObject(
                StatObjectArgs.builder()
                    .bucket(path.fileSystem.server.bucket)
                    .`object`(path.objectKey)
                    .build()
            )
            S3Attributes.file(stat.size(), stat.lastModified().toInstant().toEpochMilli())
        } catch (_: Exception) {
            if (isDirectory(path)) S3Attributes.directory()
            else throw IOException("S3 object not found: ${path.objectKey}")
        }
    }

    private fun isDirectory(path: S3Path): Boolean {
        if (path.objectKey.isEmpty()) return true
        val prefix = path.objectKey.trimEnd('/') + "/"
        return try {
            path.fileSystem.client.listObjects(
                ListObjectsArgs.builder()
                    .bucket(path.fileSystem.server.bucket)
                    .prefix(prefix)
                    .recursive(false)
                    .maxKeys(1)
                    .build()
            ).iterator().hasNext()
        } catch (_: Exception) {
            false
        }
    }

    private fun exists(path: S3Path): Boolean = try {
        attributes(path)
        true
    } catch (_: Exception) {
        false
    }

    private fun requireSameFileSystem(a: S3Path, b: S3Path) {
        require(a.fileSystem == b.fileSystem) { "S3 paths belong to different servers" }
    }
}

internal class S3FileSystem(
    private val provider: S3FileSystemProvider,
    val server: S3Server
) : FileSystem(), ByteStringListPathCreator, Parcelable {
    val rootDirectory = S3Path(this, "/".toByteString())
    private var open = true

    val client: MinioClient by lazy {
        MinioClient.builder()
            .endpoint(server.endpoint)
            .credentials(server.accessKey, server.secretKey)
            .build()
    }

    override fun provider(): FileSystemProvider = provider
    override fun close() {
        if (open) {
            provider.removeFileSystem(this)
            open = false
        }
    }
    override fun isOpen(): Boolean = open
    override fun isReadOnly(): Boolean = false
    override fun getSeparator(): String = "/"
    override fun getRootDirectories(): Iterable<Path> = listOf(rootDirectory)
    override fun getFileStores(): Iterable<FileStore> = emptyList()
    override fun supportedFileAttributeViews(): Set<String> = setOf("basic")
    override fun getPath(first: String, vararg more: String): S3Path =
        S3Path(this, buildString {
            append(first)
            more.forEach {
                if (isNotEmpty() && !endsWith('/')) append('/')
                append(it)
            }
        }.toByteString())
    override fun getPath(first: ByteString, vararg more: ByteString): S3Path =
        S3Path(this, buildString {
            append(first)
            more.forEach {
                if (isNotEmpty() && last() != '/') append('/')
                append(it)
            }
        }.toByteString())
    override fun getPathMatcher(syntaxAndPattern: String): PathMatcher = throw UnsupportedOperationException()
    override fun getUserPrincipalLookupService() = throw UnsupportedOperationException()
    override fun newWatchService(): WatchService = LocalWatchService()
    override fun equals(other: Any?): Boolean = other is S3FileSystem && server.id == other.server.id
    override fun hashCode(): Int = server.id.hashCode()
    override fun describeContents(): Int = 0
    override fun writeToParcel(dest: Parcel, flags: Int) = dest.writeLong(server.id)

    companion object CREATOR : Parcelable.Creator<S3FileSystem> {
        override fun createFromParcel(source: Parcel): S3FileSystem =
            S3FileSystemProvider.getOrNewFileSystem(source.readLong())
        override fun newArray(size: Int): Array<S3FileSystem?> = arrayOfNulls(size)
    }
}

internal class S3Path : ByteStringListPath<S3Path> {
    val fileSystem: S3FileSystem

    constructor(fileSystem: S3FileSystem, path: ByteString) : super('/', path) {
        this.fileSystem = fileSystem
    }

    private constructor(fileSystem: S3FileSystem, absolute: Boolean, segments: List<ByteString>) :
        super('/', absolute, segments) {
        this.fileSystem = fileSystem
    }

    override fun isPathAbsolute(path: ByteString): Boolean =
        path.isNotEmpty() && path[0] == '/'.code.toByte()
    override fun createPath(path: ByteString): S3Path = S3Path(fileSystem, path)
    override fun createPath(absolute: Boolean, segments: List<ByteString>): S3Path =
        S3Path(fileSystem, absolute, segments)
    override val uriAuthority: UriAuthority
        get() = UriAuthority(null, fileSystem.server.id.toString(), null)
    override val defaultDirectory: S3Path get() = fileSystem.rootDirectory
    override fun getFileSystem(): FileSystem = fileSystem
    override fun getRoot(): S3Path? = if (isAbsolute) fileSystem.rootDirectory else null
    override fun toRealPath(vararg options: LinkOption): S3Path = this
    override fun toFile(): File = throw UnsupportedOperationException()
    override fun register(
        watcher: WatchService,
        events: Array<WatchEvent.Kind<*>>,
        vararg modifiers: WatchEvent.Modifier
    ): WatchKey {
        if (watcher !is LocalWatchService) throw ProviderMismatchException(watcher.toString())
        return watcher.register(this, events, *modifiers)
    }

    val objectKey: String get() = toString().trimStart('/')
}

private class S3ByteChannel(
    private val delegate: SeekableByteChannel,
    private val temp: File,
    private val path: S3Path,
    private val writable: Boolean
) : SeekableByteChannel by delegate {
    override fun close() {
        var failure: Throwable? = null
        try {
            delegate.close()
            if (writable) {
                path.fileSystem.client.uploadObject(
                    UploadObjectArgs.builder()
                        .bucket(path.fileSystem.server.bucket)
                        .`object`(path.objectKey)
                        .filename(temp.absolutePath)
                        .build()
                )
            }
        } catch (e: Exception) {
            failure = e
        } finally {
            temp.delete()
        }
        if (failure != null) throw IOException("Unable to close S3 object ${path.objectKey}", failure)
    }
}

private class S3Attributes(
    private val directory: Boolean,
    private val objectSize: Long,
    private val modifiedMillis: Long
) : BasicFileAttributes {
    override fun lastModifiedTime(): FileTime = FileTime.fromMillis(modifiedMillis)
    override fun lastAccessTime(): FileTime = FileTime.fromMillis(modifiedMillis)
    override fun creationTime(): FileTime = FileTime.fromMillis(modifiedMillis)
    override fun isRegularFile(): Boolean = !directory
    override fun isDirectory(): Boolean = directory
    override fun isSymbolicLink(): Boolean = false
    override fun isOther(): Boolean = false
    override fun size(): Long = if (directory) 0 else objectSize
    override fun fileKey(): Any? = null

    companion object {
        fun file(size: Long, modifiedMillis: Long) = S3Attributes(false, size, modifiedMillis)
        fun directory() = S3Attributes(true, 0, 0)
    }
}

private class S3BasicFileAttributeView(
    private val path: S3Path
) : BasicFileAttributeView {
    override fun name(): String = "basic"
    override fun readAttributes(): BasicFileAttributes = S3FileSystemProvider.attributes(path)
    override fun setTimes(lastModifiedTime: FileTime?, lastAccessTime: FileTime?, createTime: FileTime?) =
        throw UnsupportedOperationException()
}
