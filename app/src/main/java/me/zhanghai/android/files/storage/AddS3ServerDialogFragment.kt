/*
 * Copyright (c) 2026 Hartraft
 * All Rights Reserved.
 */

package me.zhanghai.android.files.storage

import android.app.Dialog
import android.content.Context
import android.content.DialogInterface
import android.os.Bundle
import android.text.InputType
import android.view.ViewGroup
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.Toast
import androidx.appcompat.app.AppCompatDialogFragment
import androidx.lifecycle.lifecycleScope
import com.google.android.material.dialog.MaterialAlertDialogBuilder
import io.minio.BucketExistsArgs
import io.minio.MinioClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import me.zhanghai.android.files.provider.s3.S3FileSystemProvider
import kotlin.math.roundToInt

class AddS3ServerDialogFragment : AppCompatDialogFragment() {
    private val server: S3Server?
        get() = arguments?.getParcelable(ARG_SERVER)

    private lateinit var endpoint: EditText
    private lateinit var accessKey: EditText
    private lateinit var secretKey: EditText
    private lateinit var bucket: EditText
    private lateinit var path: EditText
    private lateinit var name: EditText

    override fun onCreateDialog(savedInstanceState: Bundle?): Dialog {
        val context = requireContext()
        val padding = (16 * resources.displayMetrics.density).roundToInt()
        val existing = server
        val content = LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(padding, padding, padding, padding)
        }
        endpoint = edit(context, "Endpoint", existing?.endpoint ?: "http://192.168.1.10:9000")
        accessKey = edit(context, "Access key", existing?.accessKey)
        secretKey = edit(context, "Secret key", existing?.secretKey).apply {
            inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_PASSWORD
        }
        bucket = edit(context, "Bucket", existing?.bucket)
        path = edit(context, "Folder (optional)", existing?.relativePath)
        name = edit(context, "Display name (optional)", existing?.customName)
        listOf(endpoint, accessKey, secretKey, bucket, path, name).forEach {
            content.addView(it, LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT
            ).apply { bottomMargin = padding / 2 })
        }
        val scroll = ScrollView(context).apply { addView(content) }
        return MaterialAlertDialogBuilder(context, theme)
            .setTitle(if (existing == null) "Add S3-compatible storage" else "Edit S3-compatible storage")
            .setView(scroll)
            .setPositiveButton(if (existing == null) "Test and add" else "Test and save", null)
            .setNegativeButton(if (existing == null) "Cancel" else "Remove", null)
            .create()
            .also { dialog ->
                dialog.setOnShowListener {
                    dialog.getButton(DialogInterface.BUTTON_POSITIVE).setOnClickListener {
                        testAndSave(dialog)
                    }
                    if (existing != null) {
                        dialog.getButton(DialogInterface.BUTTON_NEGATIVE).setOnClickListener {
                            Storages.remove(existing)
                            S3FileSystemProvider.unregister(existing)
                            Toast.makeText(requireContext(), "S3 storage removed", Toast.LENGTH_SHORT).show()
                            dialog.dismiss()
                            requireActivity().finish()
                        }
                    }
                }
            }
    }

    private fun edit(context: Context, hint: String, value: String? = null) = EditText(context).apply {
        this.hint = hint
        if (value != null) setText(value)
        setSingleLine(true)
    }

    private fun testAndSave(dialog: Dialog) {
        val endpointValue = endpoint.text.toString().trim()
            .let { if (it.startsWith("http://") || it.startsWith("https://")) it else "http://$it" }
            .trimEnd('/')
        val accessKeyValue = accessKey.text.toString().trim()
        val secretKeyValue = secretKey.text.toString()
        val bucketValue = bucket.text.toString().trim()
        val pathValue = path.text.toString().trim('/')
        val nameValue = name.text.toString().trim().ifEmpty { null }
        if (accessKeyValue.isEmpty() || secretKeyValue.isEmpty() || bucketValue.isEmpty()) {
            Toast.makeText(requireContext(), "Access key, secret key and bucket are required", Toast.LENGTH_LONG).show()
            return
        }
        lifecycleScope.launch {
            try {
                val ok = withContext(Dispatchers.IO) {
                    MinioClient.builder().endpoint(endpointValue)
                        .credentials(accessKeyValue, secretKeyValue).build()
                        .bucketExists(BucketExistsArgs.builder().bucket(bucketValue).build())
                }
                if (!ok) throw IllegalStateException("Bucket does not exist or is not accessible")
                val updated = S3Server(server?.id, nameValue, endpointValue, accessKeyValue,
                    secretKeyValue, bucketValue, pathValue)
                Storages.addOrReplace(updated)
                S3FileSystemProvider.register(updated)
                Toast.makeText(requireContext(), if (server == null) "Connected" else "Saved", Toast.LENGTH_SHORT).show()
                dialog.dismiss()
                requireActivity().finish()
            } catch (e: Exception) {
                e.printStackTrace()
                Toast.makeText(requireContext(),
                    "S3 connection failed: ${e.message ?: e.javaClass.simpleName}",
                    Toast.LENGTH_LONG).show()
            }
        }
    }

    companion object {
        private const val ARG_SERVER = "server"

        fun newInstance(server: S3Server) = AddS3ServerDialogFragment().apply {
            arguments = Bundle().apply { putParcelable(ARG_SERVER, server) }
        }
    }
}
