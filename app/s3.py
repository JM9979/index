import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging
from urllib.parse import urlparse
from app.config import config

import base64
import os
import tempfile

class S3Uploader:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
        """
        初始化S3客户端
        :param aws_access_key_id: AWS访问密钥ID
        :param aws_secret_access_key: AWS秘密访问密钥
        :param region_name: AWS区域
        """
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id or config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=aws_secret_access_key or config.AWS_SECRET_ACCESS_KEY,
            region_name=region_name or config.AWS_REGION_NAME
        )
        self.logger = logging.getLogger(__name__)

    def check_object_exists(self, object_name, bucket_name=None):
        """
        检查S3上是否已经存在指定对象
        :param object_name: 对象键名称
        :param bucket_name: 桶名称，如果未指定则使用默认桶
        :return: 如果对象存在返回True，否则返回False
        """
        try:
            # 如果没有指定bucket_name，则使用配置中的默认值
            if bucket_name is None:
                bucket_name = config.S3_BUCKET_NAME
            
            # 尝试获取对象的元数据，如果成功则说明对象存在
            self.s3_client.head_object(Bucket=bucket_name, Key=object_name)
            self.logger.info(f"对象已存在: {bucket_name}/{object_name}")
            return True
        except ClientError as e:
            # 如果对象不存在，会抛出404错误
            if e.response['Error']['Code'] == '404':
                self.logger.info(f"对象不存在: {bucket_name}/{object_name}")
                return False
            else:
                self.logger.error(f"检查对象时出错: {e.response['Error']['Message']}")
                return False
        except Exception as e:
            self.logger.error(f"检查对象时发生未知错误: {str(e)}")
            return False
            
    def upload_image(self, bucket_name=None, file_path=None, object_name=None, content_type='image/jpeg', 
                    acl='private', storage_class='STANDARD', metadata=None, tags=None):
        """
        上传图片到S3
        :param bucket_name: 桶名称
        :param file_path: 本地文件路径
        :param object_name: S3对象名称(可选)
        :param content_type: 内容类型
        :param acl: 访问控制列表
        :param storage_class: 存储类
        :param metadata: 元数据字典
        :param tags: 标签字典(如{'key1':'value1', 'key2':'value2'})
        :return: 上传成功返回True和对象键，失败返回False和错误信息
        """
        try:
            # 如果没有指定bucket_name，则使用配置中的默认值
            if bucket_name is None:
                bucket_name = config.S3_BUCKET_NAME
                
            # 如果没有指定object_name，则使用文件名称
            if object_name is None:
                object_name = file_path.split('/')[-1]

            extra_args = {
                'ContentType': content_type,
                # 'ACL': acl,
                'StorageClass': storage_class
            }

            if metadata:
                extra_args['Metadata'] = metadata

            if tags:
                # 将标签字典转换为字符串格式: 'key1=value1&key2=value2'
                tag_string = '&'.join([f"{k}={v}" for k, v in tags.items()])
                extra_args['Tagging'] = tag_string

            with open(file_path, 'rb') as file:
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=object_name,
                    Body=file,
                    **extra_args
                )

            self.logger.info(f"成功上传图片到 {bucket_name}/{object_name}")
            return True, object_name

        except FileNotFoundError:
            error_msg = f"本地文件未找到: {file_path}"
            self.logger.error(error_msg)
            return False, error_msg
        except NoCredentialsError:
            error_msg = "AWS凭证未配置或无效"
            self.logger.error(error_msg)
            return False, error_msg
        except ClientError as e:
            error_msg = f"AWS客户端错误: {e.response['Error']['Message']}"
            self.logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"上传过程中发生未知错误: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg

    def get_object_url(self, bucket_name=None, object_name=None, expires_in=3600):
        """
        获取对象的预签名URL
        :param bucket_name: 桶名称
        :param object_name: 对象键
        :param expires_in: URL过期时间(秒)，默认1小时
        :return: 预签名URL
        """
        try:
            # 如果没有指定bucket_name，则使用配置中的默认值
            if bucket_name is None:
                bucket_name = config.S3_BUCKET_NAME
                
            # 生成预签名URL
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': object_name
                },
                ExpiresIn=expires_in
            )
            
            # 如果是虚拟托管样式URL，可以转换为更友好的格式
            if '.amazonaws.com' in url:
                parsed = urlparse(url)
                if parsed.netloc.startswith(bucket_name + '.s3.'):
                    friendly_url = f"https://{bucket_name}.s3.amazonaws.com/{object_name}"
                    return friendly_url
            
            return url
        except ClientError as e:
            error_msg = f"生成URL时出错: {e.response['Error']['Message']}"
            self.logger.error(error_msg)
            return None
        except Exception as e:
            error_msg = f"获取URL时发生未知错误: {str(e)}"
            self.logger.error(error_msg)
            return None

    def get_public_url(self, bucket_name=None, object_name=None):
        """
        获取公共可访问的URL（如果对象ACL设置为public-read）
        :param bucket_name: 桶名称
        :param object_name: 对象键
        :return: 公共URL或None
        """
        try:
            # 如果没有指定bucket_name，则使用配置中的默认值
            if bucket_name is None:
                bucket_name = config.S3_BUCKET_NAME
                
            # 检查桶区域以构建正确的端点
            location = self.s3_client.get_bucket_location(Bucket=bucket_name)['LocationConstraint']
            
            # 如果桶在us-east-1，LocationConstraint会是None
            if location is None:
                location = 'us-east-1'
                
            return f"https://{bucket_name}.s3.{location}.amazonaws.com/{object_name}"
        except ClientError as e:
            self.logger.error(f"获取桶位置时出错: {e.response['Error']['Message']}")
            return None



s3_uploader = S3Uploader()

def upload_base64_image_to_s3(image_data, object_name, content_type='image/jpeg'):
    """
    上传base64编码的图片到S3存储
    
    Args:
        image_data: base64编码的图片数据，格式如"data:image/jpeg;base64,/9j/4AAQSkZ..."
        object_name: S3中的对象名称，如"collections/xyz.jpg"
        content_type: 文件的内容类型，默认为'image/jpeg'
        
    Returns:
        tuple: (success, result)
            - success: 布尔值，表示上传是否成功
            - result: 成功时为图片URL，失败时为原始图片数据
    """
    if not image_data.startswith('data:image'):
        return False, image_data
    
    try:
        # 先检查S3上是否已经存在该对象
        if s3_uploader.check_object_exists(object_name):
            # 对象已存在，直接获取URL
            image_url = s3_uploader.get_public_url(object_name=object_name)
            logging.info("Image already exists in S3, reusing: %s", image_url)
            return True, image_url
            
        # 如果不存在，则进行上传
        # 解析Base64图片数据
        image_data_encoded = image_data.split(',')[1]
        image_bytes = base64.b64decode(image_data_encoded)
        
        # 创建临时文件保存图片
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(image_bytes)
        
        # 上传到S3
        success, _ = s3_uploader.upload_image(
            file_path=temp_file_path,
            object_name=object_name,
            content_type=content_type
        )
        
        # 删除临时文件
        os.unlink(temp_file_path)
        
        if success:
            # 获取公共URL
            image_url = s3_uploader.get_public_url(object_name=object_name)
            logging.info("Image uploaded to S3: %s", image_url)
            return True, image_url
        else:
            return False, image_data
            
    except Exception as e:
        logging.error("Error uploading image to S3: %s", str(e))
        return False, image_data


# 使用示例
# if __name__ == "__main__":
#     # 配置日志
#     logging.basicConfig(level=logging.INFO)
    
#     # 初始化上传器（不需要传入参数，将使用config.py中的配置）
#     uploader = S3Uploader()
    
#     # 上传图片
#     success, object_key = uploader.upload_image(
#         file_path='/workspaces/TBC-API/index/image.jpg',
#         object_name='images/uploaded_image.jpg',  # 可选，指定S3上的路径
#         content_type='image/jpeg',
#         acl='public-read',  # 设置为公开可读
#         metadata={'author': 'your-name', 'description': 'sample image'},
#         tags={'category': 'photos', 'project': 'demo'}
#     )
    
#     if success:
#         print(f"图片上传成功，对象键: {object_key}")
        
#         # 获取预签名URL（适用于私有对象）
#         presigned_url = uploader.get_object_url(object_name=object_key)
#         print(f"预签名URL（1小时内有效）: {presigned_url}")
        
#         # 获取公共URL（如果ACL设置为public-read）
#         public_url = uploader.get_public_url(object_name=object_key)
#         print(f"公共访问URL: {public_url}")
#     else:
#         print(f"上传失败: {object_key}")  # 这里object_key实际上是错误信息