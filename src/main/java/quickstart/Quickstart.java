package quickstart;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.ContainerCreateResponse;
import com.microsoft.azure.storage.blob.models.ContainerListBlobFlatSegmentResponse;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.util.FlowableUtil;

import io.reactivex.*;
import io.reactivex.Flowable;

public class Quickstart {

    public static final String ACCOUNT_NAME = System.getenv("AZURE_STORAGE_ACCOUNT");
    public static final String ACCOUNT_KEY = System.getenv("AZURE_STORAGE_ACCESS_KEY");
    public static final String CONTAINER_NAME = "quickstart";

    static File createTempFile(String filename, String extension) throws IOException {

        // Here we are creating a temporary file to use for download and upload to Blob storage
        File sampleFile = null;
        sampleFile = File.createTempFile(filename, extension);
        System.out.println(">> Creating a sample file at: " + sampleFile.toString());
        Writer output = new BufferedWriter(new FileWriter(sampleFile));
        output.write("Hello Azure!");
        output.close();

        return sampleFile;
    }

    static File downloadFromURL(String urlString, String filename, String extension) throws IOException {
        URL url = new URL(urlString);
        ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
        FileOutputStream fileOutputStream = new FileOutputStream(String.format("%s%s", filename, extension));
        FileChannel fileChannel = fileOutputStream.getChannel();
        fileOutputStream.getChannel()
                .transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
        return new File(String.format("%s%s", filename, extension));
    }

    static File createTempZipFile(String filename, String extension) throws IOException {

        List<String> srcFiles = Arrays.asList("test1", "test2");
        String fileExtension = ".txt";
        FileOutputStream fos = new FileOutputStream(String.format("%s%s", filename, extension));
        ZipOutputStream zipOut = new ZipOutputStream(fos);
        for (String srcFile : srcFiles) {
            File fileToZip = createTempFile(srcFile, fileExtension);
            FileInputStream fis = new FileInputStream(fileToZip);
            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
            zipOut.putNextEntry(zipEntry);

            byte[] bytes = new byte[1024];
            int length;
            while((length = fis.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
            fis.close();
        }
        zipOut.close();
        fos.close();

        return new File(String.format("%s%s", filename, extension));
    }

    static String getSASURL(String blobName) throws InvalidKeyException, MalformedURLException {
        SharedKeyCredentials credential = new SharedKeyCredentials(ACCOUNT_NAME, ACCOUNT_KEY);
        ServiceSASSignatureValues values = new ServiceSASSignatureValues()
                .withProtocol(SASProtocol.HTTPS_ONLY)
                .withExpiryTime(OffsetDateTime.now().plusDays(2))
                .withContainerName(CONTAINER_NAME)
                .withBlobName(blobName);

        BlobSASPermission permission = new BlobSASPermission()
                .withRead(true)
                .withAdd(true);
        values.withPermissions(permission.toString());

        SASQueryParameters params = values.generateSASQueryParameters(credential);

        String encodedParams = params.encode();

        return String.format(Locale.ROOT, "https://%s.blob.core.windows.net/%s/%s%s",
                ACCOUNT_NAME, CONTAINER_NAME, blobName, encodedParams);
    }

    static void uploadFile(BlockBlobURL blob, File sourceFile) throws IOException {

            AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(sourceFile.toPath());

            // Uploading a file to the blobURL using the high-level methods available in TransferManager class
            // Alternatively call the PutBlob/PutBlock low-level methods from BlockBlobURL type
            TransferManager.uploadFileToBlockBlob(fileChannel, blob, 8*1024*1024, null)
            .subscribe(response-> {
                System.out.println("Completed upload request.");
                System.out.println(response.response().statusCode());
            });
    }

    static void listBlobs(ContainerURL containerURL) {
        // Each ContainerURL.listBlobsFlatSegment call return up to maxResults (maxResults=10 passed into ListBlobOptions below).
        // To list all Blobs, we are creating a helper static method called listAllBlobs,
    	// and calling it after the initial listBlobsFlatSegment call
        ListBlobsOptions options = new ListBlobsOptions();
        options.withMaxResults(10);

        containerURL.listBlobsFlatSegment(null, options, null).flatMap(containerListBlobFlatSegmentResponse ->
            listAllBlobs(containerURL, containerListBlobFlatSegmentResponse))
            .subscribe(response-> {
                System.out.println("Completed list blobs request.");
                System.out.println(response.statusCode());
            });
    }

    private static Single <ContainerListBlobFlatSegmentResponse> listAllBlobs(ContainerURL url, ContainerListBlobFlatSegmentResponse response) {
        // Process the blobs returned in this result segment (if the segment is empty, blobs() will be null.
        if (response.body().segment() != null) {
            for (BlobItem b : response.body().segment().blobItems()) {
                String output = "Blob name: " + b.name();
                if (b.snapshot() != null) {
                    output += ", Snapshot: " + b.snapshot();
                }
                System.out.println(output);
            }
        }
        else {
            System.out.println("There are no more blobs to list off.");
        }

        // If there is not another segment, return this response as the final response.
        if (response.body().nextMarker() == null) {
            return Single.just(response);
        } else {
            /*
            IMPORTANT: ListBlobsFlatSegment returns the start of the next segment; you MUST use this to get the next
            segment (after processing the current result segment
            */

            String nextMarker = response.body().nextMarker();

            /*
            The presence of the marker indicates that there are more blobs to list, so we make another call to
            listBlobsFlatSegment and pass the result through this helper function.
            */

            return url.listBlobsFlatSegment(nextMarker, new ListBlobsOptions().withMaxResults(10), null)
                    .flatMap(containersListBlobFlatSegmentResponse ->
                            listAllBlobs(url, containersListBlobFlatSegmentResponse));
        }
    }

    static void deleteBlob(BlockBlobURL blobURL) {
        // Delete the blob
        blobURL.delete(null, null, null)
        .subscribe(
            response -> System.out.println(">> Blob deleted: " + blobURL),
            error -> System.out.println(">> An error encountered during deleteBlob: " + error.getMessage()));
    }

    static void getBlob(BlockBlobURL blobURL, File sourceFile) {
        try {
            // Get the blob using the low-level download method in BlockBlobURL type
            // com.microsoft.rest.v2.util.FlowableUtil is a static class that contains helpers to work with Flowable
            // BlobRange is defined from 0 to 4MB
            blobURL.download(new BlobRange().withOffset(0).withCount(4*1024*1024L), null, false, null)
                    .flatMapCompletable(response -> {
                        AsynchronousFileChannel channel = AsynchronousFileChannel.open(Paths.get(sourceFile.getPath()), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                        return FlowableUtil.writeFile(response.body(null), channel);
                    }).doOnComplete(()-> System.out.println("The blob was downloaded to " + sourceFile.getAbsolutePath()))
                    // To call it synchronously add .blockingAwait()
                    .subscribe();
        } catch (Exception ex){

            System.out.println(ex.toString());
        }
    }

    public static void main(String[] args) throws java.lang.Exception{
        ContainerURL containerURL;

        // Creating a sample file to use in the sample
        File sampleZipFile = null;

        try {

            String filename = "SampleArchiveFromURL";
            String extension = ".zip";
            String url = "https://testblaub.blob.core.windows.net/quickstart/SampleArchive.zip?sv=2018-03-28&spr=https&se=2018-10-13T22%3A57%3A40Z&sr=b&sp=ra&sig=CwtNboHxmHQart0gjoajgm7HWsWYnSllARWwiw3zvBA%3D";
            sampleZipFile = downloadFromURL(url, "test", ".zip");



            File downloadedFile = File.createTempFile("downloadedFile", ".txt");


            // Create a ServiceURL to call the Blob service. We will also use this to construct the ContainerURL
            SharedKeyCredentials creds = new SharedKeyCredentials(ACCOUNT_NAME, ACCOUNT_KEY);
            // We are using a default pipeline here, you can learn more about it at https://github.com/Azure/azure-storage-java/wiki/Azure-Storage-Java-V10-Overview
            final ServiceURL serviceURL = new ServiceURL(new URL("https://" + ACCOUNT_NAME + ".blob.core.windows.net"), StorageURL.createPipeline(creds, new PipelineOptions()));

            // Let's create a container using a blocking call to Azure Storage
            // If container exists, we'll catch and continue
            containerURL = serviceURL.createContainerURL(CONTAINER_NAME);

            try {
                ContainerCreateResponse response = containerURL.create(null, null, null).blockingGet();
                System.out.println("Container Create Response was " + response.statusCode());
            } catch (RestException e){
                if (e instanceof RestException && ((RestException)e).response().statusCode() != 409) {
                    throw e;
                } else {
                    System.out.println("quickstart container already exists, resuming...");
                }
            }

            // Create a BlockBlobURL to run operations on Blobs
            String blobName = String.format("%s%s", filename, extension);
            final BlockBlobURL blobURL = containerURL.createBlockBlobURL(blobName);

            // Listening for commands from the console
            System.out.println("Enter a command");
            System.out.println("(P)utBlob | (L)istBlobs | (G)etBlob | (D)eleteBlobs | (E)xitSample");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            while (true) {

                System.out.println("# Enter a command : ");
                String input = reader.readLine();

                switch(input){
                    case "P":
                        System.out.println("Uploading the sample file into the container: " + containerURL );
                        uploadFile(blobURL, sampleZipFile);
                        System.out.println("SAS Url: " + getSASURL(blobName));
                        break;
                    case "L":
                        System.out.println("Listing blobs in the container: " + containerURL );
                        listBlobs(containerURL);
                        break;
                    case "G":
                        System.out.println("Get the blob: " + blobURL.toString() );
                        getBlob(blobURL, downloadedFile);
                        break;
                    case "D":
                        System.out.println("Delete the blob: " + blobURL.toString() );
                        deleteBlob(blobURL);
                        System.out.println();
                        break;
                    case "E":
                        System.out.println("Cleaning up the sample and exiting!");
                        containerURL.delete(null, null).blockingGet();
                        sampleZipFile.delete();
                        downloadedFile.delete();
                        System.exit(0);
                        break;
                    default:
                        break;
                }
            }
        } catch (InvalidKeyException e) {
            System.out.println("Invalid Storage account name/key provided");
        } catch (MalformedURLException e) {
            System.out.println("Invalid URI provided");
        } catch (RestException e){
            System.out.println("Service error returned: " + e.response().statusCode() );
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
